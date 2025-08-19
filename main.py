import boto3
import pandas as pd

from Importer import Importer


def main():
    imp = Importer()
    imp.import_s3_file('Talent/10391.json')

if __name__ == "__main__":
    main()