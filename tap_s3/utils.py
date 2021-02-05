import pandas as pd


def infer_type(data):
    try:
        int(data)
        return int
    except (ValueError, TypeError):
        pass
    try:
        float(data)
        return float
    except (ValueError, TypeError):
        pass
    return str

def clean_dataframe(df):
    for column, column_vals in df.iteritems():
        inferred_type = str
        valid_value_index = column_vals.first_valid_index()
        if valid_value_index is not None:
            raw_value = column_vals[valid_value_index]
            inferred_type = infer_type(raw_value)
        if inferred_type == int:
            casted_vals = []
            for val in column_vals:
                if pd.notna(val):
                    casted_vals.append(int(val))
                else:
                    casted_vals.append(val)
            df[column] = pd.Series(casted_vals, dtype=pd.Int64Dtype())
        elif inferred_type == float:
            df = df.astype({column: float})
        else:
            pass
    return df