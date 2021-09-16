# MyPy for Static Typing
from typing import List, Set, Dict, Tuple, Optional, Any, Iterable

# Custom Modules
from helpers.config import config, schedulers
from helpers.logger import logger
from lib.models import Pool
from lib.types.ITableStructure import ITableStructure

# PyPi Modules
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import statistics
import pandas as pd
from pandas.core.frame import DataFrame

class Baseline:
    def __init__(self) -> None:
        Pool.acquire()
        try:        
            self.execute()    
        finally:
            Pool.release()   
    def execute(self):
        self.Sql: ReturnSql = ReturnSql()
        try:
            logger.info('Retrieving Data')
            data: DataFrame = self.__retrieveData()
            logger.info('Successfully retrieved data')
        except Exception as error:
            logger.error(f'Failed to retrieve data, with error: {error}')
            raise 
        try:
            logger.info('Calulating regression models for data')
            regressor = self.__createFormula(data[['variable1','variable2','variable3']], data['outputvariable'])
            data['prediction'] = regressor.predict(data[['variable1','variable2','variable3']])
        except Exception as error:
            logger.error(f'failed to calulate regression models for data, with error: {error}')
            raise

        try:
            logger.info('Loading calulated prediction values into database')
            self.__insert(data[['variable1','variable2','variable3','outputvariable','prediction']])
            logger.info('Successfully loaded all calulated prediction values into database')
        except Exception as error:
            logger.error(f'failed to load all calulated prediction values into database, with error: {error}')
            raise

    def __insert(self, data: list):
        try:    
            Pool.executeMany(
                sql=self.Sql.insertPredictedValues(),
                batchData=data,
                batchLimit=1000
            )
        except Exception as error:
            logger.error(f'Unable to insert prediction values into mysql')                         
            raise

    def __retrieveData(self) -> DataFrame:
        data: List[ITableStructure] = Pool.getData(self.Sql.retrieveData)
        return pd.DataFrame(data = data)

    def __removeOutliers(self, df: DataFrame, columnName: str):   
        if df.empty: return df            
        df[columnName] = pd.to_numeric(df[columnName], errors='coerce')
        q1, q3 = df[columnName].quantile(0.25), df[columnName].quantile(0.75)
        interQuar = q3 - q1
        upperLimit = q3 + (interQuar * 1.5)
        lowerLimit = q1 - (interQuar * 1.5)

        df = df[df[columnName].between(lowerLimit, upperLimit)]
        if df.empty: logger.warning('There is no remaining data for this meter after the outliers have been removed')
        logger.debug(f'{len(df.values)}: records remaing after the outlier removal process')
        return df

    def __createFormula(self, x: DataFrame, y: DataFrame) -> dict:   
        res = []
        for randomStateValue in range(0, 10000, 200):
            # Splitting data into training and test data
            xTrain, xTest, yTrain, yTest = train_test_split(x.values, y.values, test_size = 0.1, random_state = randomStateValue)        
            # Produce regressor
            regressor = LinearRegression(fit_intercept= True, normalize=False)
            # Fit regressor to our training data
            regressor.fit(xTrain, yTrain)
            rSquared: float = regressor.score(xTest, yTest)
            resDict: dict = {
                    'randomStateValue' : randomStateValue,
                    'rSquared' : rSquared
            }    
            res.append(resDict)

        # Choosing random state value with highest R2
        resDf: DataFrame = pd.DataFrame(data=res)
        chosenRandomStateValue = resDf[(resDf['rSquared'] == resDf['rSquared'].max())]['randomStateValue'].values[0]

        # Now that we have the random state value, lets produce the regressor for this data
        xTrain, xTest, yTrain, yTest = train_test_split(x.values, y.values, test_size = 0.1, random_state = chosenRandomStateValue)      
        regressor = LinearRegression(fit_intercept= True, normalize=False)
        regressor.fit(xTrain, yTrain)        
        return regressor


class ReturnSql:
    def __init__(self) -> None:
        self = self
    
    def retrieveData(self) -> str:
        return """
            select variable1,
                variable2,
                variable3,
                outputvariable
            from data
        """
    def insertPredictedValues(self) -> str:
        return """        
            insert into predictions
                (variable1, variable2, variable3, outputvariable, prediction)
            VALUES
                (%s, %s, %s, %s, %s)
        """      


