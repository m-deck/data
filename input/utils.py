import sys
import datetime

import numpy
import pandas
from pandas import DataFrame
from pathlib import Path
from scipy import optimize

import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import seaborn as sns
sns.set()

def _series_converter(series: pandas.Series):
    if series.name == 'Estimated':
        return series.astype(float)
    if series.name == 'Confirmed' or series.name == 'Deaths':
        return series.astype(float).astype('Int64')
    else:
        return series.fillna('').astype(str)

def dataframe_output(data: DataFrame, root: Path, name: str = None, metadata_merge: str = 'inner'):
    '''
    This function performs the following steps:
    1. Sorts the dataset by date and country / region
    2. Merges the data with country / region metadata
    '''
    # Core columns are those that appear in all datasets and can be used for merging with metadata
    core_columns = pandas.read_csv(root / 'input' / 'output_columns.csv').columns.tolist()
    pivot_columns = core_columns[:-5]

    # Merge with metadata from appropriate helper dataset
    # Data from https://developers.google.com/public-data/docs/canonical/countries_csv and Wikipedia
    metadata = pandas.read_csv(root / 'input' / ('metadata_%s.csv' % name), dtype=str)
    meta_columns = [col for col in metadata.columns
                    if col not in pivot_columns and not col.startswith('_')]
    data = data.merge(metadata, how=metadata_merge)

    # If a column does not exist in the dataset, output empty values
    for column in core_columns:
        if column not in data.columns: data[column] = ''

    # Make sure the core columns have the right data type
    for column in data.columns: data[column] = _series_converter(data[column])

    # Preserve the order of the core columns, which must be recomputed after merging with metadata
    all_columns = pivot_columns + meta_columns
    extra_columns = [col for col in all_columns if col not in core_columns]
    data = data[core_columns + extra_columns]

    # Make sure the dataset is properly sorted
    data = data.sort_values(core_columns)

    # Output time-series dataset to sys.out
    data.to_csv(sys.stdout, header=None, index=False)

# Helper function used to filter out uninteresting dates
def get_outbreak_mask(data: DataFrame, threshold: int = 10):
    ''' Returns a mask for > N confirmed cases '''
    return data['Confirmed'] > threshold

# Define prediction model
def _logistic_function(X: float, a: float, b: float, c: float):
    ''' a * e^(-b * e^(-cx)) '''
    return a * numpy.exp(-b * numpy.exp(-c * X))

def _forward_indices(indices: list, window: int):
    ''' Adds `window` indices to a list of dates '''
    date_indices = [datetime.date.fromisoformat(date) for date in indices]
    for _ in range(window): date_indices.append(date_indices[-1] + datetime.timedelta(days=1))
    return [date.isoformat() for date in date_indices]

# Main work function for each subset of data
def compute_forecast(data: pandas.Series, window: int):
    '''
    Perform a forecast of `window` days past the last day of `data`, including a model estimate of
    all days already existing in `data`.
    '''

    # Some of the parameter fittings result in overflow
    numpy.seterr(all='ignore')

    # Perform a simple fit of all available data up to this date
    X, y = list(range(len(data))), data.tolist()
    # Providing a reasonable initial guess is crucial for this model
    params, _ = optimize.curve_fit(
        _logistic_function, X, y, maxfev=int(1E6), p0=[max(y), numpy.median(X), .1])

    # Append N new days to our indices
    date_indices = _forward_indices(data.index, window)

    # Perform projection with the previously estimated parameters
    projected = [_logistic_function(x, *params) for x in range(len(X) + window)]
    return pandas.Series(projected, index=date_indices, name='Estimated')

def _plot_options():
    return {'figsize': (16, 8), 'fontsize': 'x-large', 'grid': True}

def _plot_save(fname: str, ax):
    # Add legend
    ax.legend(loc='upper left', fontsize='x-large')
    # Remove X label
    ax.xaxis.set_label_text('')
    # Make Y axis int only
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    # Save the figure
    ax.get_figure().tight_layout()
    ax.get_figure().savefig(fname)
    # Close the figure
    plt.close(ax.get_figure())

def plot_column(fname: str, data: pandas.Series):
    df = DataFrame({data.name: data})
    ax = df.plot(kind='bar', **_plot_options())
    _plot_save(fname, ax)

def plot_forecast(fname: str, confirmed: pandas.Series, estimated: pandas.Series):

    # Replace all the indices from data with zeroes in our projected data
    projected = estimated.copy()
    projected[confirmed.dropna().index] = 0

    # Add new date indices to the original data and fill them with zeroes
    confirmed = confirmed.copy()
    for index in sorted(set(estimated.index) - set(confirmed.index)):
        confirmed.loc[index] = 0

    df = DataFrame({'Confirmed': confirmed, 'Projected': projected})
    ax = df.plot(kind='bar', **_plot_options())
    ax.plot(estimated.index, estimated, color='red', label='Estimate')
    _plot_save(fname, ax)
