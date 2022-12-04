
import planetary_computer
import pystac_client
import pystac
import xarray as xr
import numpy as np
import pandas as pd
from dask.diagnostics import ProgressBar
from tqdm.auto import tqdm
from xarray.core.variable import Variable
import os


class DataExtractor():
  
  def __init__(
      self,
      path=None,
      var="tasmax", 
      bbox=[129.408463169,145.543137242,31.0295791692,45.5514834662], 
      time_range=["2020-01-01", "2022-01-05"],
      exp_id="ssp585"
      ):
    self.var = var
    self.bbox = bbox
    self.time_range = time_range
    self.exp_id = exp_id
    self.path = os.getcwd() if path == None else path
    self.dataset = None

  def get_cmip(self):
    '''
    Download CMIP6 data from planetary computer
    '''
    catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    search = catalog.search(
        collections=["cil-gdpcir-cc-by"],
        query={"cmip6:source_id": {"eq": "NESM3"}, "cmip6:experiment_id": {"eq": self.exp_id}},
    )

    items = search.get_all_items()
    assert len(items) == 1
    item = planetary_computer.sign(items[0])

    asset = item.assets[self.var]
    ds = xr.open_dataset(asset.href, **asset.extra_fields["xarray:open_kwargs"])

    subset = ds.sel(
      lon=slice(self.bbox[0], self.bbox[1]),
      lat=slice(self.bbox[2], self.bbox[3]),
      time=slice(self.time_range[0], self.time_range[1]),
    )

    with ProgressBar():
        subset = subset.compute()

    self.dataset = subset
    return subset

  def export_csv(self):
    '''
    Export spatial mean time series to csv file
    '''
    ds = self.dataset
    assert ds != None, "No dataset found!"
    
    df = ds[self.var].mean(['lat','lon']).to_dataframe()
    df.to_csv(os.path.join(self.path,f"{self.var}.csv"))
    
    print("Data Exported!")



def main():
    '''
    Main controller for setting up extractor.
    '''
    extractor = DataExtractor(bbox=[11,12,47,49]) # munich
    ds = extractor.get_cmip()
    extractor.export_csv()

	
if __name__ == '__main__':
    main()