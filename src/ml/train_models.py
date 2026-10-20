import os
import sys
import time
import json
import gc
import yaml
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from src.utils.spark_session import get_spark_session
from src.ml.guard import validate_feature_selection

def fast_auc_roc(y_true, y_score):
    pos_mask = (y_true == 1)
    n_pos = int(np.sum(pos_mask))
    n_neg = len(y_true) - n_pos
    if n_pos == 0 or n_neg == 0:
        return 0.5
    rank = np.argsort(np.argsort(y_score)) + 1
    u_pos = np.sum(rank[pos_mask]) - n_pos * (n_pos + 1) / 2.0
    return float(u_pos / (n_pos * n_neg))

def fast_auc_pr(y_true, y_score):
    n_pos = int(np.sum(y_true == 1))
    if n_pos == 0:
        return 0.0
    order = np.argsort(-y_score)
    y_sorted = y_true[order]
    tp_cumsum = np.cumsum(y_sorted == 1)
    precision = tp_cumsum / (np.arange(len(y_sorted)) + 1)
    return float(np.mean(precision[y_sorted == 1]))

def get_metrics_at_threshold(y_true, y_prob, threshold):
    preds = (y_prob >= threshold).astype(int)
    tp = int(np.sum((preds == 1) & (y_true == 1)))
    fp = int(np.sum((preds == 1) & (y_true == 0)))
    tn = int(np.sum((preds == 0) & (y_true == 0)))
    fn = int(np.sum((preds == 0) & (y_true == 1)))
    
    total = len(y_true)
    accuracy = (tp + tn) / total if total > 0 else 0.0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    
    return {
        "threshold": float(round(threshold, 4)),
        "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "accuracy": float(round(accuracy, 4)),
        "precision": float(round(precision, 4)),
        "recall": float(round(recall, 4)),
        "f1": float(round(f1, 4))
    }

def find_best_thresholds(y_true, y_prob):
    thresholds = np.linspace(0.05, 0.85, 81)
    best_f1 = -1.0
    th_max_f1 = 0.50
    metrics_max_f1 = None
    
    th_recall_60 = None
    diff_recall_60 = 999.0
    metrics_recall_60 = None
    
    for th in thresholds:
        m = get_metrics_at_threshold(y_true, y_prob, th)
        if m["f1"] > best_f1:
            best_f1 = m["f1"]
            th_max_f1 = th
            metrics_max_f1 = m
            
        diff = abs(m["recall"] - 0.60)
        if diff < diff_recall_60:
            diff_recall_60 = diff
            th_recall_60 = th
            metrics_recall_60 = m
            
    return th_max_f1, metrics_max_f1, th_recall_60, metrics_recall_60

def compute_decile_calibration(y_true, y_prob, num_bins=10):
    quantiles = np.percentile(y_prob, np.linspace(0, 100, num_bins + 1))
    bins = []
    for i in range(num_bins):
        low = quantiles[i]
        high = quantiles[i+1]
        if i == num_bins - 1:
            idx = (y_prob >= low) & (y_prob <= high)
        else:
            idx = (y_prob >= low) & (y_prob < high)
            
        count = int(np.sum(idx))
        if count > 0:
            mean_pred = float(np.mean(y_prob[idx]))
            mean_actual = float(np.mean(y_true[idx]))
            abs_err = abs(mean_pred - mean_actual)
        else:
            mean_pred = 0.0
            mean_actual = 0.0
            abs_err = 0.0
            
        bins.append({
            "decile": i + 1,
            "prob_range": f"[{low:.4f}, {high:.4f}]",
            "count": count,
            "mean_pred": round(mean_pred, 4),
            "actual_rate": round(mean_actual, 4),
            "calib_error": round(abs_err, 4)
        })
    return bins

def main():
    start_total = time.perf_counter()
    os.makedirs("logs/metrics", exist_ok=True)
    os.makedirs("logs/verify/phase6", exist_ok=True)
    
    with open("config/ml_features.yaml") as f:
        ml_cfg = yaml.safe_load(f)
        
    cat_cols = ml_cfg["features"]["categorical"]
    num_cols = ml_cfg["features"]["numeric"]
    validate_feature_selection(cat_cols + num_cols)
    print("Features validated against guard.")
    
    n_train = 4299046
    n_val = 482878
    n_test = 932084
    base_hdfs = "hdfs://localhost:9000/user/saniya/flight_delay/processed/ml/dataset"
    models_base = "hdfs://localhost:9000/user/saniya/flight_delay/processed/ml/models"
    
    # ==========================================================================
    # SESSION 1: BASELINES & LOGISTIC REGRESSION
    # ==========================================================================
    print("\n" + "="*80)
    print("SESSION 1: BASELINES & LOGISTIC REGRESSION (master=local[4])")
    print("="*80)
    spark1 = get_spark_session("train_lr", extra_configs={
        "spark.master": "local[4]",
        "spark.driver.memory": "4g",
        "spark.sql.shuffle.partitions": "16"
    })
    
    train_df = spark1.read.parquet(f"{base_hdfs}/train")
    val_df = spark1.read.parquet(f"{base_hdfs}/val")
    test_df = spark1.read.parquet(f"{base_hdfs}/test")
    
    # 1. Baselines
    n_pos_train = 836017
    global_train_rate = n_pos_train / n_train
    print(f"Global TRAIN Delay Rate: {global_train_rate:.4f} ({global_train_rate*100:.2f}%)")
    
    m_smooth = 20.0
    cell_stats = (train_df.groupBy("AIRLINE", "SCHED_DEP_HOUR")
                  .agg(F.count("label").alias("n_cell"), F.sum("label").alias("pos_cell"))
                  .withColumn("rate_score", (F.col("pos_cell") + F.lit(m_smooth * global_train_rate)) / (F.col("n_cell") + F.lit(m_smooth))))
    
    val_rate_baseline = (val_df.join(cell_stats, on=["AIRLINE", "SCHED_DEP_HOUR"], how="left")
                         .fillna({"rate_score": global_train_rate}))
    test_rate_baseline = (test_df.join(cell_stats, on=["AIRLINE", "SCHED_DEP_HOUR"], how="left")
                          .fillna({"rate_score": global_train_rate}))
                          
    eval_auc = BinaryClassificationEvaluator(rawPredictionCol="rate_score", labelCol="label", metricName="areaUnderROC")
    eval_pr = BinaryClassificationEvaluator(rawPredictionCol="rate_score", labelCol="label", metricName="areaUnderPR")
    
    val_rate_auc = eval_auc.evaluate(val_rate_baseline)
    test_rate_auc = eval_auc.evaluate(test_rate_baseline)
    test_rate_pr = eval_pr.evaluate(test_rate_baseline)
    
    test_rows_base = test_rate_baseline.select("label", "rate_score").collect()
    y_test = np.array([r["label"] for r in test_rows_base], dtype=float)
    y_test_rate = np.array([r["rate_score"] for r in test_rows_base], dtype=float)
    
    maj_m = get_metrics_at_threshold(y_test, np.zeros_like(y_test), 0.5)
    
    val_rows_base = val_rate_baseline.select("label", "rate_score").collect()
    y_val = np.array([r["label"] for r in val_rows_base], dtype=float)
    y_val_rate = np.array([r["rate_score"] for r in val_rows_base], dtype=float)
    
    th_rate_f1, _, th_rate_rec60, _ = find_best_thresholds(y_val, y_val_rate)
    rate_m_def = get_metrics_at_threshold(y_test, y_test_rate, 0.5)
    rate_m_tuned = get_metrics_at_threshold(y_test, y_test_rate, th_rate_f1)
    rate_m_rec60 = get_metrics_at_threshold(y_test, y_test_rate, th_rate_rec60)
    
    baselines_metrics = {
        "majority": {
            "test_accuracy": maj_m["accuracy"], "test_precision": maj_m["precision"],
            "test_recall": maj_m["recall"], "test_f1": maj_m["f1"], "auc_roc": 0.5000
        },
        "rate_baseline": {
            "val_auc_roc": round(val_rate_auc, 4), "test_auc_roc": round(test_rate_auc, 4),
            "test_auc_pr": round(test_rate_pr, 4),
            "th_max_f1": round(float(th_rate_f1), 2),
            "metrics_at_max_f1": rate_m_tuned,
            "metrics_at_recall_60": rate_m_rec60
        }
    }
    with open("logs/metrics/ml_baselines.json", "w") as f:
        json.dump(baselines_metrics, f, indent=2)
    print(f"Majority Baseline on Test: Accuracy={maj_m['accuracy']:.4f}, Precision={maj_m['precision']:.4f}, Recall={maj_m['recall']:.4f}, F1={maj_m['f1']:.4f}, AUC-ROC=0.5000")
    print(f"Rate Baseline on Test:     AUC-ROC={test_rate_auc:.4f}, AUC-PR={test_rate_pr:.4f}, F1 (th={th_rate_f1:.2f})={rate_m_tuned['f1']:.4f}")
    
    # 2. Logistic Regression
    start_lr = time.perf_counter()
    n_neg_train = n_train - n_pos_train
    w_pos = n_train / (2.0 * n_pos_train)
    w_neg = n_train / (2.0 * n_neg_train)
    train_weighted = train_df.withColumn("weight", F.when(F.col("label") == 1.0, F.lit(w_pos)).otherwise(F.lit(w_neg)))
    
    lr_indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
    lr_ohe = OneHotEncoder(inputCols=[f"{c}_idx" for c in cat_cols], outputCols=[f"{c}_vec" for c in cat_cols], dropLast=False)
    lr_assembler = VectorAssembler(inputCols=[f"{c}_vec" for c in cat_cols] + num_cols, outputCol="raw_features")
    lr_scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)
    
    lr_prep_pipe = Pipeline(stages=lr_indexers + [lr_ohe, lr_assembler, lr_scaler])
    lr_prep_model = lr_prep_pipe.fit(train_weighted)
    
    train_lr_ready = lr_prep_model.transform(train_weighted).select("features", "label", "weight")
    val_lr_ready = lr_prep_model.transform(val_df).select("features", "label")
    test_lr_ready = lr_prep_model.transform(test_df)
    
    grid_params = [
        {"regParam": 0.01, "elasticNetParam": 0.0},
        {"regParam": 0.1,  "elasticNetParam": 0.0},
        {"regParam": 0.01, "elasticNetParam": 0.5}
    ]
    best_lr_auc = -1.0
    best_lr_model = None
    best_lr_cfg = None
    eval_auc_mllib = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC")
    
    print("\nTuning Logistic Regression grid on VALIDATION:")
    for cfg in grid_params:
        lr_est = LogisticRegression(featuresCol="features", labelCol="label", weightCol="weight",
                                    maxIter=15, regParam=cfg["regParam"], elasticNetParam=cfg["elasticNetParam"])
        m = lr_est.fit(train_lr_ready)
        v_preds = m.transform(val_lr_ready)
        auc_val = eval_auc_mllib.evaluate(v_preds)
        print(f"  regParam={cfg['regParam']}, elasticNetParam={cfg['elasticNetParam']} -> Val AUC-ROC = {auc_val:.4f}")
        if auc_val > best_lr_auc:
            best_lr_auc = auc_val
            best_lr_model = m
            best_lr_cfg = cfg
            
    print(f"Selected Best LR: regParam={best_lr_cfg['regParam']}, elasticNetParam={best_lr_cfg['elasticNetParam']} with Val AUC = {best_lr_auc:.4f}")
    
    val_preds_lr = best_lr_model.transform(val_lr_ready).select("label", "probability").collect()
    y_val_lr_prob = np.array([float(r["probability"][1]) for r in val_preds_lr])
    y_val_true = np.array([float(r["label"]) for r in val_preds_lr])
    
    lr_th_f1, lr_m_val_f1, lr_th_rec60, lr_m_val_rec60 = find_best_thresholds(y_val_true, y_val_lr_prob)
    
    test_preds_lr = best_lr_model.transform(test_lr_ready)
    test_lr_auc = eval_auc_mllib.evaluate(test_preds_lr)
    eval_pr_mllib = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderPR")
    test_lr_pr = eval_pr_mllib.evaluate(test_preds_lr)
    
    test_rows_lr = test_preds_lr.select("FLIGHT_DATE", "MONTH", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT",
                                        "SCHED_DEP_HOUR", "label", "probability").collect()
    y_test_lr_prob = np.array([float(r["probability"][1]) for r in test_rows_lr])
    
    lr_test_def = get_metrics_at_threshold(y_test, y_test_lr_prob, 0.50)
    lr_test_tuned = get_metrics_at_threshold(y_test, y_test_lr_prob, lr_th_f1)
    lr_test_rec60 = get_metrics_at_threshold(y_test, y_test_lr_prob, lr_th_rec60)
    lr_calib = compute_decile_calibration(y_test, y_test_lr_prob)
    
    attrs = []
    for idx_stage in lr_indexers:
        labels = lr_prep_model.stages[cat_cols.index(idx_stage.getInputCol())].labels
        col_name = idx_stage.getInputCol()
        for l in labels:
            attrs.append(f"{col_name}={l}")
    attrs.extend(num_cols)
    
    lr_coefs = best_lr_model.coefficients.toArray()
    coef_pairs = sorted(zip(attrs[:len(lr_coefs)], lr_coefs), key=lambda x: x[1], reverse=True)
    top_pos_coefs = [{"feature": p[0], "coefficient": round(float(p[1]), 4)} for p in coef_pairs[:10]]
    top_neg_coefs = [{"feature": p[0], "coefficient": round(float(p[1]), 4)} for p in coef_pairs[-10:]]
    
    lr_duration = time.perf_counter() - start_lr
    lr_metrics = {
        "model": "LogisticRegression",
        "duration_seconds": round(lr_duration, 2),
        "best_hyperparams": best_lr_cfg,
        "validation_auc_roc": round(best_lr_auc, 4),
        "test_auc_roc": round(test_lr_auc, 4),
        "test_auc_pr": round(test_lr_pr, 4),
        "test_base_rate": round(float(np.mean(y_test)), 4),
        "threshold_max_f1": round(float(lr_th_f1), 2),
        "metrics_at_max_f1": lr_test_tuned,
        "threshold_recall_60": round(float(lr_th_rec60), 2),
        "metrics_at_recall_60": lr_test_rec60,
        "metrics_default_0_5": lr_test_def,
        "decile_calibration": lr_calib,
        "top_positive_coefficients": top_pos_coefs,
        "top_negative_coefficients": top_neg_coefs
    }
    with open("logs/metrics/ml_logistic_regression.json", "w") as f:
        json.dump(lr_metrics, f, indent=2)
    print(f"Logistic Regression Test Evaluation: AUC-ROC={test_lr_auc:.4f}, AUC-PR={test_lr_pr:.4f}, F1 (th={lr_th_f1:.2f})={lr_test_tuned['f1']:.4f}")
    
    # Save LR model and predictions
    print(f"Persisting Logistic Regression model to {models_base}/logistic_regression...")
    best_lr_model.write().overwrite().save(f"{models_base}/logistic_regression")
    
    prob_udf = F.udf(lambda v: float(v[1]), DoubleType())
    pred_lr_th_udf = F.udf(lambda v: 1.0 if float(v[1]) >= float(lr_th_f1) else 0.0, DoubleType())
    
    preds_lr_export = (test_preds_lr.select(
        F.col("FLIGHT_DATE"), F.col("AIRLINE"), F.col("ORIGIN_AIRPORT"),
        F.col("DESTINATION_AIRPORT"), F.col("SCHED_DEP_HOUR"), F.col("label"),
        prob_udf("probability").alias("probability"),
        pred_lr_th_udf("probability").alias("prediction"),
        F.lit("LogisticRegression").alias("model")
    ))
    preds_lr_export.write.mode("overwrite").parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/ml/predictions_test_lr")
    print("Saved LR predictions.")
    
    # Extract metadata for monthly & airline breakdown
    test_months = np.array([r["MONTH"] for r in test_rows_lr], dtype=int)
    test_airlines = np.array([r["AIRLINE"] for r in test_rows_lr])
    
    # Clean shutdown of Session 1
    spark1.stop()
    del spark1, train_df, val_df, test_df, train_weighted, train_lr_ready, val_lr_ready, test_lr_ready, best_lr_model
    gc.collect()
    print("Session 1 stopped and JVM memory cleared.")
    
    # ==========================================================================
    # SESSION 2: GBT CLASSIFIER (FRESH JVM INSTANCE)
    # ==========================================================================
    print("\n" + "="*80)
    print("SESSION 2: GBT CLASSIFIER (master=local[4], fresh JVM)")
    print("="*80)
    start_gbt = time.perf_counter()
    spark2 = get_spark_session("train_gbt", extra_configs={
        "spark.master": "local[4]",
        "spark.driver.memory": "4g",
        "spark.sql.shuffle.partitions": "16"
    })
    
    train_df = spark2.read.parquet(f"{base_hdfs}/train")
    val_df = spark2.read.parquet(f"{base_hdfs}/val")
    test_df = spark2.read.parquet(f"{base_hdfs}/test")
    
    # Representative sample of 50,000 rows (seed 42) for WSL2 memory stability
    gbt_sample_size = 50000
    gbt_sample_fraction = gbt_sample_size / n_train
    print(f"Sampling {gbt_sample_size:,} rows ({gbt_sample_fraction*100:.2f}%) from TRAIN for GBT (seed=42)...")
    train_gbt_sample = train_df.sample(withReplacement=False, fraction=gbt_sample_fraction, seed=42)
    
    gbt_indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
    gbt_assembler = VectorAssembler(inputCols=[f"{c}_idx" for c in cat_cols] + num_cols, outputCol="features")
    gbt_pipe = Pipeline(stages=gbt_indexers + [gbt_assembler])
    
    print("Fitting GBT feature pipeline...")
    gbt_prep_model = gbt_pipe.fit(train_gbt_sample)
    train_gbt_ready = gbt_prep_model.transform(train_gbt_sample).select("features", "label")
    val_gbt_ready = gbt_prep_model.transform(val_df).select("features", "label")
    test_gbt_ready = gbt_prep_model.transform(test_df)
    
    # Grid search on VALIDATION for GBT: stepSize 0.1 vs 0.05
    print("Tuning GBTClassifier grid on VALIDATION (maxBins=400, maxDepth=6, maxIter=40)...")
    gbt_grid = [
        {"stepSize": 0.1, "maxDepth": 6, "maxIter": 40, "maxBins": 400},
        {"stepSize": 0.05, "maxDepth": 6, "maxIter": 40, "maxBins": 400}
    ]
    best_gbt_auc = -1.0
    best_gbt_model = None
    best_gbt_cfg = None
    eval_auc_mllib2 = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderROC")
    
    for cfg in gbt_grid:
        gbt_est = GBTClassifier(featuresCol="features", labelCol="label", maxBins=cfg["maxBins"],
                                maxDepth=cfg["maxDepth"], maxIter=cfg["maxIter"], stepSize=cfg["stepSize"], seed=42)
        m = gbt_est.fit(train_gbt_ready)
        v_preds = m.transform(val_gbt_ready)
        auc_val = eval_auc_mllib2.evaluate(v_preds)
        print(f"  stepSize={cfg['stepSize']}, maxDepth={cfg['maxDepth']}, maxIter={cfg['maxIter']} -> Val AUC-ROC = {auc_val:.4f}")
        if auc_val > best_gbt_auc:
            best_gbt_auc = auc_val
            best_gbt_model = m
            best_gbt_cfg = cfg
            
    print(f"Selected Best GBT: stepSize={best_gbt_cfg['stepSize']} with Val AUC = {best_gbt_auc:.4f}")
    
    val_preds_gbt = best_gbt_model.transform(val_gbt_ready)
    val_rows_gbt = val_preds_gbt.select("label", "probability").collect()
    y_val_gbt_prob = np.array([float(r["probability"][1]) for r in val_rows_gbt])
    
    gbt_th_f1, gbt_m_val_f1, gbt_th_rec60, gbt_m_val_rec60 = find_best_thresholds(y_val_true, y_val_gbt_prob)
    print(f"GBT Validation Thresholds: Max F1 Th={gbt_th_f1:.2f} (Val F1={gbt_m_val_f1['f1']:.4f}), 60% Recall Th={gbt_th_rec60:.2f} (Val Recall={gbt_m_val_rec60['recall']:.4f})")
    
    test_preds_gbt = best_gbt_model.transform(test_gbt_ready)
    test_gbt_auc = eval_auc_mllib2.evaluate(test_preds_gbt)
    eval_pr_mllib2 = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label", metricName="areaUnderPR")
    test_gbt_pr = eval_pr_mllib2.evaluate(test_preds_gbt)
    
    test_rows_gbt = test_preds_gbt.select("FLIGHT_DATE", "MONTH", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT",
                                          "SCHED_DEP_HOUR", "label", "probability").collect()
    y_test_gbt_prob = np.array([float(r["probability"][1]) for r in test_rows_gbt])
    
    if test_gbt_auc > 0.85:
        print(f"WARNING: Suspected leakage detected! GBT Test AUC = {test_gbt_auc:.4f} > 0.85")
    else:
        print(f"PASS: GBT Test AUC = {test_gbt_auc:.4f} <= 0.85 (No leakage)")
        
    gbt_test_def = get_metrics_at_threshold(y_test, y_test_gbt_prob, 0.50)
    gbt_test_tuned = get_metrics_at_threshold(y_test, y_test_gbt_prob, gbt_th_f1)
    gbt_test_rec60 = get_metrics_at_threshold(y_test, y_test_gbt_prob, gbt_th_rec60)
    gbt_calib = compute_decile_calibration(y_test, y_test_gbt_prob)
    
    gbt_feat_names = [f"{c}_idx" for c in cat_cols] + num_cols
    importances = best_gbt_model.featureImportances.toArray()
    gbt_imp_list = sorted(zip(gbt_feat_names, importances), key=lambda x: x[1], reverse=True)
    gbt_imp_report = [{"feature": f[0].replace("_idx", ""), "importance": round(float(f[1]), 4)} for f in gbt_imp_list]
    
    gbt_duration = time.perf_counter() - start_gbt
    gbt_metrics = {
        "model": "GBTClassifier",
        "sample_strategy": f"Trained on 50,000 rows (representative sample of TRAIN, seed=42) for memory safety",
        "hyperparameters": best_gbt_cfg,
        "duration_seconds": round(gbt_duration, 2),
        "validation_auc_roc": round(best_gbt_auc, 4),
        "test_auc_roc": round(test_gbt_auc, 4),
        "test_auc_pr": round(test_gbt_pr, 4),
        "test_base_rate": round(float(np.mean(y_test)), 4),
        "threshold_max_f1": round(float(gbt_th_f1), 2),
        "metrics_at_max_f1": gbt_test_tuned,
        "threshold_recall_60": round(float(gbt_th_rec60), 2),
        "metrics_at_recall_60": gbt_test_rec60,
        "metrics_default_0_5": gbt_test_def,
        "decile_calibration": gbt_calib,
        "feature_importances": gbt_imp_report
    }
    with open("logs/metrics/ml_gbt.json", "w") as f:
        json.dump(gbt_metrics, f, indent=2)
    print(f"GBT Classifier Test Evaluation: AUC-ROC={test_gbt_auc:.4f}, AUC-PR={test_gbt_pr:.4f}, F1 (th={gbt_th_f1:.2f})={gbt_test_tuned['f1']:.4f}")
    
    print(f"Persisting GBT model to {models_base}/gbt...")
    best_gbt_model.write().overwrite().save(f"{models_base}/gbt")
    
    pred_gbt_th_udf = F.udf(lambda v: 1.0 if float(v[1]) >= float(gbt_th_f1) else 0.0, DoubleType())
    preds_gbt_export = (test_preds_gbt.select(
        F.col("FLIGHT_DATE"), F.col("AIRLINE"), F.col("ORIGIN_AIRPORT"),
        F.col("DESTINATION_AIRPORT"), F.col("SCHED_DEP_HOUR"), F.col("label"),
        prob_udf("probability").alias("probability"),
        pred_gbt_th_udf("probability").alias("prediction"),
        F.lit("GBTClassifier").alias("model")
    ))
    preds_gbt_export.write.mode("overwrite").parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/ml/predictions_test_gbt")
    print("Saved GBT predictions.")
    
    # ==========================================================================
    # COMBINE PREDICTIONS & METRICS
    # ==========================================================================
    print("\nMerging test predictions and writing final outputs...")
    p_lr = spark2.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/ml/predictions_test_lr")
    p_gbt = spark2.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/ml/predictions_test_gbt")
    p_combined = p_lr.unionByName(p_gbt)
    p_combined.write.mode("overwrite").parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/ml/predictions_test")
    print("Combined test predictions persisted to processed/ml/predictions_test.")
    
    # Monthly breakdown & Per-airline evaluation
    monthly_breakdown = {}
    for m, m_name in [(11, "November"), (12, "December")]:
        mask = (test_months == m)
        m_y_true = y_test[mask]
        m_prob_lr = y_test_lr_prob[mask]
        m_prob_gbt = y_test_gbt_prob[mask]
        
        auc_lr = fast_auc_roc(m_y_true, m_prob_lr)
        pr_lr = fast_auc_pr(m_y_true, m_prob_lr)
        auc_gbt = fast_auc_roc(m_y_true, m_prob_gbt)
        pr_gbt = fast_auc_pr(m_y_true, m_prob_gbt)
        
        monthly_breakdown[m_name] = {
            "month": m,
            "flight_count": int(np.sum(mask)),
            "delayed_count": int(np.sum(m_y_true)),
            "base_rate": round(float(np.mean(m_y_true)), 4),
            "lr": {
                "auc_roc": round(float(auc_lr), 4),
                "auc_pr": round(float(pr_lr), 4),
                "metrics_max_f1": get_metrics_at_threshold(m_y_true, m_prob_lr, lr_th_f1)
            },
            "gbt": {
                "auc_roc": round(float(auc_gbt), 4),
                "auc_pr": round(float(pr_gbt), 4),
                "metrics_max_f1": get_metrics_at_threshold(m_y_true, m_prob_gbt, gbt_th_f1)
            }
        }
        print(f"Month {m} ({m_name:<8}): Base Rate={monthly_breakdown[m_name]['base_rate']*100:.2f}%, LR AUC={auc_lr:.4f}, GBT AUC={auc_gbt:.4f}")
        
    unique_airlines = sorted(list(set(test_airlines)))
    per_airline = []
    print("\nPer-Airline Test Performance:")
    for al in unique_airlines:
        mask = (test_airlines == al)
        al_y_true = y_test[mask]
        al_prob_lr = y_test_lr_prob[mask]
        al_prob_gbt = y_test_gbt_prob[mask]
        
        cnt = int(np.sum(mask))
        delayed = int(np.sum(al_y_true))
        rate = delayed / cnt if cnt > 0 else 0.0
        
        auc_lr = fast_auc_roc(al_y_true, al_prob_lr)
        auc_gbt = fast_auc_roc(al_y_true, al_prob_gbt)
            
        per_airline.append({
            "airline": al,
            "flight_count": cnt,
            "delayed_count": delayed,
            "delay_rate": round(rate, 4),
            "lr_auc_roc": round(float(auc_lr), 4),
            "gbt_auc_roc": round(float(auc_gbt), 4)
        })
        print(f"  {al:<4}: {cnt:>6,} flights | Delay Rate: {rate*100:>5.2f}% | LR AUC: {auc_lr:.4f} | GBT AUC: {auc_gbt:.4f}")
        
    test_summary_metrics = {
        "monthly_breakdown": monthly_breakdown,
        "per_airline": per_airline
    }
    with open("logs/metrics/ml_test_summary.json", "w") as f:
        json.dump(test_summary_metrics, f, indent=2)
        
    metrics_records = [
        ("MajorityBaseline", "none", 0.5000, 0.0, maj_m["accuracy"], maj_m["precision"], maj_m["recall"], maj_m["f1"]),
        ("RateBaseline", f"th={th_rate_f1:.2f}", test_rate_auc, test_rate_pr, rate_m_tuned["accuracy"], rate_m_tuned["precision"], rate_m_tuned["recall"], rate_m_tuned["f1"]),
        ("LogisticRegression", f"th={lr_th_f1:.2f}", test_lr_auc, test_lr_pr, lr_test_tuned["accuracy"], lr_test_tuned["precision"], lr_test_tuned["recall"], lr_test_tuned["f1"]),
        ("GBTClassifier", f"th={gbt_th_f1:.2f}", test_gbt_auc, test_gbt_pr, gbt_test_tuned["accuracy"], gbt_test_tuned["precision"], gbt_test_tuned["recall"], gbt_test_tuned["f1"])
    ]
    metrics_schema = StructType([
        StructField("model", StringType(), False),
        StructField("threshold_strategy", StringType(), False),
        StructField("test_auc_roc", DoubleType(), False),
        StructField("test_auc_pr", DoubleType(), False),
        StructField("test_accuracy", DoubleType(), False),
        StructField("test_precision", DoubleType(), False),
        StructField("test_recall", DoubleType(), False),
        StructField("test_f1", DoubleType(), False)
    ])
    metrics_df = spark2.createDataFrame(metrics_records, metrics_schema)
    metrics_df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/ml/metrics")
    print("Metrics Parquet table persisted to processed/ml/metrics.")
    
    total_time = time.perf_counter() - start_total
    print(f"\nAll models trained, evaluated, and persisted in {total_time:.2f} seconds.")
    
    spark2.stop()

if __name__ == "__main__":
    main()
