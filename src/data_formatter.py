import pandas as pd

def highlight_selected_text(row: dict):
    text = row["item_name"]
    bold =['Chocolate']
    for k in bold:
        text = text.replace(k, f'<span style="background-color:#ddd;">{k}</span>')
    return text

