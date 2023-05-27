class Context:
    def __init__(self, spark):
        self.__spark = spark

    @property
    def spark(self):
        return self.__spark

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        return
