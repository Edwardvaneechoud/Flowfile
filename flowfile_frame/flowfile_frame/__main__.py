"""Main entry point for the FlowFrame CLI."""

def main():
    """Main entry point for the FlowFrame CLI."""
    print("FlowFrame - A Polars-like API for building ETL graphs")
    print("Usage: import flowframe as ff")
    print("       df = ff.from_dict({'a': [1, 2, 3]})")
    print("       result = df.filter(ff.col('a') > 1)")
    print("       print(result.collect())")

if __name__ == "__main__":
    main()