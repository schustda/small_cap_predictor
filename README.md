![Dropwizard](./images/logo.jpg)

### About

This repository contains an algorithm to predict a 'buy' signal for an assortment of small cap stocks.

### Getting Started

All you need is an active Gmail account and installation of python 3. The application will need to be continuously running so utilizing an instance on the cloud will aid.

### Prerequisites

On your Gmail account, ensure that 'Allow less secure apps is set to: ON'. Doing so can be done here: https://myaccount.google.com/lesssecureapps?pli=1

### Instructions for use:

Install dependencies with:

`pip install -r requirements.txt`

Begin running the application with:

`python run.py`

Enter your email address and password when prompted.

### Authors

* **Douglas Schuster**
    * github -- [schustda](https://github.com/schustda)
    * email -- [douglas.schuster303@gmail.com](douglas.schuster303@gmail.com)

### Data Sources
* [Fidelity](https://www.fidelity.com/)
* [InvestorsHub](http://investorshub.advfn.com)

### Versioning

Version 1 created 09/27/2017 (XGboost)

### Model Performance on unseen test set:

```
AUC Score: 0.89
Recall: 0.74
Precision: 0.66
```
### Sources:

http://www.marcoaltini.com/blog/dealing-with-imbalanced-data-undersampling-oversampling-and-proper-cross-validation
