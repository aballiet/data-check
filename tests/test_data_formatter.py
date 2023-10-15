import pandas as pd
from data_check.data_formatter import highlight_selected_text, highlight_diff, convert_float_to_percentage

def test_convert_float_to_percentage():
    df = pd.DataFrame({'a': [0.1, 0.2], 'b': [0.3, 1]})
    df = convert_float_to_percentage(df, ['a', 'b'])
    assert df['a'][0] == '10.00%'
    assert df['a'][1] == '20.00%'
    assert df['b'][0] == '30.00%'
    assert df['b'][1] == '100.00%'