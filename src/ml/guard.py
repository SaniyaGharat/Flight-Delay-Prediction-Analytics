import yaml

def validate_feature_selection(selected_features, feature_roles_path="config/feature_roles.yaml"):
    with open(feature_roles_path, "r") as f:
        config = yaml.safe_load(f)
    cols_meta = config.get("columns", {})
    
    forbidden_uses = {"excluded-leakage", "excluded-time-split", "label", "identifier"}
    violations = []
    for feat in selected_features:
        if feat not in cols_meta:
            continue
        model_use = cols_meta[feat].get("model_use")
        if model_use in forbidden_uses:
            violations.append(f"Feature '{feat}' has forbidden model_use '{model_use}' (role: {cols_meta[feat].get('role')})")
            
    if violations:
        raise ValueError("Feature selection validation failed:\n" + "\n".join(violations))
    return True
