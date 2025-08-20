import boto3
import pandas as pd

from Extracter import Extracter


def main():
    imp = Extracter()
    imp.import_s3_file('Talent/10391.json')

if __name__ == "__main__":
    main()