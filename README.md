## How to execute the program

### Virtual Environment 

**For Linux:**
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**For Windows:**
```bash
python -m venv .venv
.venv\Scripts\activate     
pip install -r requirements.txt
```

To execute the program, you will need to create a .env file in the main folder. An example of the .env file in the annexe of this file. After that all that is need is to run the main file and the extract, the transformation, the initialisation and the injection of the data will be done.

```commandline
python main.py
```

## ERD : Academy 

![ERD Academy](img/academy_mld.png)

## Tools
You can install the dependencies with:

```bash
pip install -r requirements.txt
```

## Annexes 

### ENV Template

Create a .env file with these parameters to connect to your mysql database

DB_DRIVER=
DB_SERVER=
DB_DATABASE=
DB_USER=
DB_PASSWORD=
