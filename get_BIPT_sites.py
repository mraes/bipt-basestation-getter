import requests, os, time, logging, json
import pandas as pd
import numpy as np
import geopandas as gpd
from ca_parser import parse_conformiteitsattest
import pdb
from tqdm.contrib.concurrent import process_map  # or thread_map
from tqdm import tqdm
from functools import partial

logger = logging.getLogger(__name__)

def get_bipt_sites_json(bbTL, bbTR, bbBL, bbBR): #top left, top right, bottom left, bottom right

    latfrom = bbTL
    latto = bbTR
    longfrom = bbBL
    longto = bbBR
    url = "https://sites.bipt.be/ajaxinterface.php"
    payload = f"action=getSites&latfrom={latfrom}&latto={latto}&longfrom={longfrom}&longto={longto}&LangSiteTable=sitesnl"
    headers = {
        'content-type': "application/x-www-form-urlencoded",
    }
    print("Fetching data from BIPT...")
    response = requests.request("POST", url, data=payload, headers=headers)
    return response.text

def get_bipt_sites_from_json (json):
    # returns a GeoDataFrame with all BIPT sites specified in the json
    df = pd.read_json(json)
    df = df.dropna(axis='columns', how='all') # clean up the data, remove all-empty columns
    print(f"BIPT JSON contains {len(df.index)} sites")

    df = df[df["Status"] == "O"] # only take Operational sites (Status = O)
    print(f"After removing non-operational sites, there are {len(df.index)} sites.")
    df = df.drop(columns=["BIPTRef1", "BIPTRef2", "BIPTRef3", "Status", "Ref1", "Ref2", "Ref3", "NrOps", "SiteType"]) # clean up the dataframe, dumping unneeded info
    df["Proximus"] = np.where(df["Eigenaar1"].str.contains("Proximus") | df["Eigenaar2"].str.contains("Proximus") | df["Eigenaar3"].str.contains("Proximus"), True, False)
    df["Orange"] = np.where(df["Eigenaar1"].str.contains("Orange") | df["Eigenaar2"].str.contains("Orange") | df["Eigenaar3"].str.contains("Orange"), True, False)
    df["Telenet"] = np.where(df["Eigenaar1"].str.contains("Telenet") | df["Eigenaar2"].str.contains("Telenet") | df["Eigenaar3"].str.contains("Telenet"), True, False)


    # Now create GeoDataFrames from the above data, in Lambert 72
    sites = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.X, df.Y), crs="EPSG:31370")
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
    # get BIPT sites between bounding box coordinates
    # Square around Leuven: 50.85403591206532&latto=50.9028849568349&longfrom=4.656774657415301&longto=4.744493621038348
    #r = getBIPTsites(1,2,3,4)


    # creating a geodataframe in Lambert 72
    sites = get_bipt_sites_from_json("data/bipt.json")

    # extracting the sites per operator (some are co-loc sites so pxs + tnt + org > total sites in most cases)
    pxs = sites[sites["Proximus"]==True] # all proximus sites
    tnt = sites[sites["Telenet"]==True] # all telenet sites
    org = sites[sites["Orange"]==True] # all orange sites
    print(f"PROXIMUS: {len(pxs.index)}, TELENET: {len(tnt.index)}, ORANGE: {len(org.index)} (Possibly with co-location)")
    if(False): # nice for displaying the selected sites
        sites.to_file("sites_bipt.geojson", driver="GeoJSON")
        pxs.to_file("sites_pxs.geojson", driver='GeoJSON')
        tnt.to_file("sites_tnt.geojson", driver='GeoJSON')
        org.to_file("sites_org.geojson", driver='GeoJSON')

    #wfs_tnt = gpd.read_file("wfs_tnt.geojson") # Extracted from QGIS
    #tnt_sites = get_features_for_sites(tnt, wfs_tnt)
    #download_attesten_for_features(tnt_sites, "data/attesten_tnt")
    #parse_attesten_for_features(tnt_sites)

    print("PROXIMUS")
    print("---------------------------------------------------------------")
    wfs_pxs = gpd.read_file("data/wfs_pxs.geojson") # Extracted from QGIS
    # TODO: filter wfs_pxs with bounding box under investigation (possible in geopandas!)
    pxs_sites = get_features_for_sites(pxs, wfs_pxs)
    download_attesten_for_features(pxs_sites, "test/attesten_pxs")
    parsed_attesten = parse_attesten_for_features(pxs_sites)
    sites_sector_list = get_sites_sectors_list(pxs_sites, parsed_attesten) # currently filters out non-L8 bands
    with open('sites_with_sectors_pxs.json', 'w') as outfile:
        json.dump(sites_sector_list, outfile, indent=4)
    print("----------------------------------------------------------------")

    print("TELENET")
    print("---------------------------------------------------------------")
    wfs_tnt = gpd.read_file("data/wfs_tnt.geojson") # Extracted from QGIS
    # TODO: filter wfs_tnt with bounding box under investigation (possible in geopandas!)
    tnt_sites = get_features_for_sites(tnt, wfs_tnt)
    tnt_sites = download_attesten_for_features(tnt_sites, "test/attesten_tnt")
    parsed_attesten = parse_attesten_for_features(tnt_sites)
    sites_sector_list = get_sites_sectors_list(tnt_sites, parsed_attesten) # currently filters out non-L8 bands
    with open('sites_with_sectors_tnt.json', 'w') as outfile:
        json.dump(sites_sector_list, outfile, indent=4)

    # TODO: GENERATE GEOJSON OF ACTUAL SITES (that are in the sites_sector_list)
    # To make maps of the simulated sites


    #wfs_org = gpd.read_file("wfs_org.geojson") # Extracted from QGIS
        