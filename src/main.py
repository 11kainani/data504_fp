import time

from db.Transformation import Transformer



def academy_db():

    start = time.perf_counter()
    transformer = Transformer()
    transformer.academy_data_injection()
    end = time.perf_counter()
    print(f"Execution time: {end - start:.4f} seconds")


if __name__ == "__main__":
    academy_db()