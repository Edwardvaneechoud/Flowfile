if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()
    from flowfile_worker.main import run

    run()
