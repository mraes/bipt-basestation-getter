import requests, os, time, logging, json
import pandas as pd
import numpy as np
import geopandas as gpd
from ca_parser import parse_conformiteitsattest
import pdb
from tqdm.contrib.concurrent import process_map  # or thread_map
from tqdm import tqdm
from functools import partial
from pyproj import Proj, transform

logger = logging.getLogger(__name__)

def get_bipt_sites_json(latfrom, latto, longfrom, longto):

    url = "https://sites.bipt.be/ajaxinterface.php"
    payload = f"action=getSites&latfrom={latfrom}&latto={latto}&longfrom={longfrom}&longto={longto}&LangSiteTable=sitesnl"
    headers = {
        'content-type': "application/x-www-form-urlencoded",
    }
    print(f"[BIPT] Fetching sites for {latfrom, latto, longfrom, longto}")
    response = requests.request("POST", url, data=payload, headers=headers)
    return response.text

def load_bipt_sites_from_json (json):
    # returns a GeoDataFrame with all BIPT sites specified in the json
    df = pd.read_json(json)
    df = df.dropna(axis='columns', how='all') # clean up the data, remove all-empty columns
    total_sites = len(df)

    df = df[df["Status"] == "O"] # only take Operational sites (Status = O)
    operational_sites_number = len(df)
    print(f"[BIPT] {total_sites} BIPT sites found, of which {operational_sites_number} operational.")

    df = df.drop(columns=["BIPTRef1", "BIPTRef2", "BIPTRef3", "Status", "Ref1", "Ref2", "Ref3", "NrOps", "SiteType"]) # clean up the dataframe, dumping unneeded info
    df["Proximus"] = np.where(df["Eigenaar1"].str.contains("Proximus") | df["Eigenaar2"].str.contains("Proximus") | df["Eigenaar3"].str.contains("Proximus"), True, False)
    df["Orange"] = np.where(df["Eigenaar1"].str.contains("Orange") | df["Eigenaar2"].str.contains("Orange") | df["Eigenaar3"].str.contains("Orange"), True, False)
    df["Telenet"] = np.where(df["Eigenaar1"].str.contains("Telenet") | df["Eigenaar2"].str.contains("Telenet") | df["Eigenaar3"].str.contains("Telenet"), True, False)

    # Now create GeoDataFrames from the above data, in Lambert 72
    sites = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.X, df.Y), crs="EPSG:31370")
    return sites

def get_wgs84_bbox_from_lambert_bbox(left, top, right, bottom):
    inProj = Proj('epsg:31370')
    outProj = Proj('epsg:4326')
    latfrom,longfrom = transform(inProj,outProj, left, bottom)
    latto, longto = transform(inProj,outProj, right, top)
    return latfrom, latto, longfrom, longto

def get_bipt_sites_in_lambert_bbox(left, top, right, bottom):
    resp = get_bipt_sites_json(*get_wgs84_bbox_from_lambert_bbox(left, top, right, bottom))
    # creating a geodataframe in Lambert 72
    sites = load_bipt_sites_from_json(resp)
    return sites

def get_features_for_sites(sites, features):
    # per BIPT site in sites, select the WFS feature that is nearby & has the most recent attest
    # returns a list with all selected features
    sites_list = []
    for index, row in sites.iterrows():
        print(f"\nSearching for Site {index} | BIPT id {row.ID} | {row.geometry}")
        #print("Closest WFS features:")
        search_range = 55 # radius in which we will look for WFS features around BIPT site locations
        closest_wfs = features[features.distance(row.geometry) < search_range].sort_values(by="goedkeuringsdatum", ascending=False)
        if(closest_wfs.empty):
            print("NO WFS FEATURES FOUND! There are no conformiteitsattesten for this site.")
            continue
            #raise Exception(f"No WFS features close to {row.geometry} | BIPT id {row.ID}")
        closest_wfs_selected = closest_wfs.iloc[0].copy()
        closest_wfs_selected["BIPTid"] = row.ID
        print(f"Selected {closest_wfs_selected.id}")
        sites_list.append(closest_wfs_selected)
    
    print(f"\n[GET_FEATURES_FOR_SITES] Returning {len(sites_list)} features for {len(sites)} BIPT sites.")
    #print(sites_list)
    return sites_list
    
def get_attest_for_site(site, directory: str):
    if(site.conformiteitsattest):
        #print(f"Getting conformiteitsattest for dossier: {site.dossiernummer}")
        os.makedirs(directory, exist_ok=True)

        path = f'{directory}/{site.dossiernummer}.pdf'
        if not os.path.exists(path):
            #print(f"Downloading: {site.conformiteitsattest}")
            r = requests.get(site.conformiteitsattest, allow_redirects=True)
            with open(path, 'wb') as f:
               f.write(r.content)
            fromcache = False
        else:
            #print("Conformiteitsattest already in cache, skipping.")
            fromcache = True
        return path, fromcache
    else:
        raise Exception("Site does not have conformiteitsattest URL")

def _download_attest(site, directory):
    filename, fromcache = get_attest_for_site(site, directory)
    fromcache_str = "[CACHE]" if fromcache else ""
    #tqdm.write(f"Download: {site.conformiteitsattest} -> {filename} {fromcache_str}")
    return filename

def download_attesten_for_features(features, directory):
    print(f"Downloading {len(features)} conformiteitsattesten to ./{directory} \n")
    list_of_files = process_map(partial(_download_attest, directory=directory), features, max_workers=os.cpu_count(), desc="Downloading")
    for index, f in enumerate(features):
        f["attest"] = list_of_files[index]
    return features


def _parse_attest(pdfpath):
    #tqdm.write(f"Parsing {pdfpath}")
    jsonpath = f"{pdfpath}.json"
    if(os.path.exists(jsonpath)):
            #tqdm.write(f"Was already parsed to {jsonpath}, loading from cache.")
            df = pd.read_json(jsonpath)
    else:
        df = parse_conformiteitsattest(pdfpath)
        #tqdm.write(df.to_string())
        df.to_json(jsonpath, orient="records")
        #tqdm.write(f"Saved to {jsonpath}") 
    return df

def parse_attesten_for_features(features):
    attest_list = []
    for f in features:
        attest_list.append(f.attest)
    print(f"Parsing {len(attest_list)} conformiteitsattesten...")
    parsed_sites = process_map(_parse_attest, attest_list, max_workers=os.cpu_count(), desc="Parsing")
    return parsed_sites

def get_sites_sectors_list(sites, attesten):
    r = []
    #bipt_ids = [site["BIPTid"] for site in sites]
    for index, site in enumerate(sites):
        sectors = attesten[index]

        # FOR TESTING, LET'S DROP ALL NON-800Mhz
        if (not sectors.empty):
            sectors = sectors[abs(sectors.Frequency - 800) < 50]

        if (sectors.empty): # no LTE sectors
            #print(f"BIPT site {site.BIPTid} has no sectors after filtering {site.attest}")
            continue
        else:
            sectors = sectors.drop(columns="Antenna").to_dict('records')
            site_dict = {'bipt_id': site.BIPTid, 'location': {'x' : site.geometry.x, 'y': site.geometry.y}, 'sectors' : sectors}
            r.append(site_dict)
    return r

if __name__ == "__main__":
    
    
    if(False): # nice for displaying the selected sites
        sites.to_file("sites_bipt.geojson", driver="GeoJSON")
        pxs.to_file("sites_pxs.geojson", driver='GeoJSON')
        tnt.to_file("sites_tnt.geojson", driver='GeoJSON')
        org.to_file("sites_org.geojson", driver='GeoJSON')

    #wfs_tnt = gpd.read_file("wfs_tnt.geojson") # Extracted from QGIS
    #tnt_sites = get_features_for_sites(tnt, wfs_tnt)
    #download_attesten_for_features(tnt_sites, "data/attesten_tnt")
    #parse_attesten_for_features(tnt_sites)


    #wfs_org = gpd.read_file("wfs_org.geojson") # Extracted from QGIS
        