# Case Study Analysis Information

Submission By - Aditya Kumar

### Analytics:

The exploratory data analysis has been carried out on the dataset comprising 6 csv files based on the set of analytic questions. Please refer to notebooks/analysis.ipynb for further insights.

The solutions to the 10 analytic questions has been developed leveraging the insights of exploratory data analysis. Please refer to notebooks/solutions.ipynb

### Usage:

* The config.yaml file at src/config/config.yaml allows to set the dataset input paths and output folder and format.

* During the execution a log file identified by the timestamp gets generated and is accessible under the /logs folder.

* All files provided as part of the case study analysis are part of the data/ folder.

* The repository can be cloned and the execution steps can be followed from the root directory of the project.

### Execution Steps [Local]:

    spark-submit --master local[*] src/main.py --config src/config/config.yaml

        