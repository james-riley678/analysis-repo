from typing import List, Set, Dict, Tuple, Optional, Any, Iterable

import numpy as np
import sys
import os
import pandas as pd

import datetime
from itertools import chain

# Modules for function encodeCategoricalData
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder

def encodeCategoricalData(data: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    # Encoding categorical data
    ct = ColumnTransformer(transformers=[('encoder', OneHotEncoder(sparse = False), [0])])
    data = np.array(ct.fit_transform(data.values))
    data = pd.DataFrame(data = data, columns = [x.replace('encoder__x0_','') for x in ct.get_feature_names()])
    return data

from itertools import tee, zip_longest

def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip_longest(a, b, fillvalue=None)
    
def cd(d) -> Any:
    try:
        return datetime.datetime.strptime(d, '%Y-%m-%d').date()       
    except:
        return d

def collapseRanges(sorted_iterable, inc):
    pairs = pairwise(sorted_iterable)
    for start, tmp in pairs:
        if inc(start) == tmp:
            for end, tmp in pairs:
                if inc(end) != tmp:
                    break
            yield start, end
        else:
            yield start, start
def groupSequence(groupList):      
    oneDay = datetime.timedelta(days=1)
    returnList = []
    for each in collapseRanges(sorted(set(groupList)), lambda d: cd(d) + oneDay):
        returnList.append(each)
    return returnList       

class Error(Exception):
    pass

