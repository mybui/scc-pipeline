from pipeline import *
from validate import validate_dataframe

def main():
    # raw, bronze, and gold pipeline
    load_to_raw()
    load_to_bronze()
    load_to_silver()
    df_pdau = load_to_gold()

    # validate data before publish visualisation
    validate_dataframe(df_pdau)

if __name__ == "__main__":
    main()