"""
Main entry point for the FlowFile package.
"""


def main():
    """
    Display information about FlowFile when run directly as a module.
    """
    import flowfile

    print(f"FlowFile v{flowfile.__version__}")
    print("A framework combining visual ETL with a Polars-like API")
    print("\nUsage examples:")
    print("  import flowfile as ff")
    print("  df = ff.read_csv('data.csv')")
    print("  result = df.filter(ff.col('value') > 10)")
    print("  result.write_csv('output.csv')")
    print("\nFor visual ETL:")
    print("  ff.open_graph_in_editor(result.to_graph())")


if __name__ == "__main__":
    main()