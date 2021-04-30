from typing import Tuple
import sys, getopt
from pathlib import Path
import logging

from geopandas.geodataframe import GeoDataFrame
from numpy import remainder, void
from BIPT_sites import get_bipt_sites_in_lambert_bbox, load_bipt_sites_from_json, get_features_for_sites, download_attesten_for_features, parse_attesten_for_features, get_sites_sectors_list
import geopandas as gpd
import json
import pdb
import requests, os, time, logging, json, datetime

DATA_DIR = "./data"
OUTPUT_DIR = "./gent"

def get_zendantennes_wfs(bbox: Tuple) -> GeoDataFrame:
    #download all features from VL Zendantenneskaart https://zendantenneskaart.omgeving.vlaanderen.be/
    wfs_path = f"{DATA_DIR}/zendantennes.geojson"
    if(not os.path.exists(wfs_path)):
        print(f"{wfs_path} does not exist. Downloading!")
        url = "https://www.mercator.vlaanderen.be/raadpleegdienstenmercatorpubliek/us/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=us:us_zndant_pnt&outputFormat=application/json"
        response = requests.get(url)
        with open(f'{DATA_DIR}/zendantennes.geojson', 'w') as outfile:
            outfile.write(response.text)
    else:
        last_updated_at = datetime.datetime.fromtimestamp(os.path.getmtime(wfs_path))
        print(f"{wfs_path} exists, using that. Last updated at {last_updated_at}")
    return gpd.read_file(wfs_path, bbox=bbox)

def print_bipt_sites_statistics(bipt_sites) -> void:
    s = bipt_sites
    pxs = s["Proximus"] # proximus site selector
    tnt = s["Telenet"]  # telenet site selector
    org = s["Orange"]   # orange site selector
    coloc = (pxs & org) | (pxs & tnt) | (org & tnt)
    print(f"Total sites: {len(bipt_sites)}, {len(s[pxs & org & tnt])} sites with all operators present.")
    print(f"{len(s[coloc])} sites with colocation")
    print(f"Proximus: {len(s[pxs])}, {len(s[pxs & tnt])} with TNT, {len(s[pxs & org])} with ORG")
    print(f"Telenet: {len(s[tnt])}, {len(s[tnt & pxs])} with PXS, {len(s[tnt & org])} with ORG.")
    print(f"Orange: {len(s[org])}, {len(s[org & pxs])} with PXS, {len(s[org & tnt])} with TNT.")
   
if __name__ == "__main__":

    opts, remainder = getopt.getopt(sys.argv[1:], "b:o:")
    bbox = (102843.5303, 191922.9061, 106843.5303, 195922.9061)

    for opt, arg in opts:
        if opt == "-b":
            bbox = [float(c) for c in arg.split(",")]
        elif opt == "-o":
            OUTPUT_DIR = Path.cwd() / arg

    print(f"BBOX: {bbox}")
    print(f"OUTPUT_DIR: {OUTPUT_DIR}")
    Path.mkdir(OUTPUT_DIR, exist_ok=True)
    # Get the WFS features (in GeoDataFrame) in the above bbox from vlaanderen.be
    zendantennes = get_zendantennes_wfs(bbox)
    pxs_wfs = zendantennes[zendantennes["operatornaam"] == "Proximus NV"]
    tnt_wfs = zendantennes[zendantennes["operatornaam"] == "Telenet Group BVBA"]
    org_wfs = zendantennes[zendantennes["operatornaam"] == "Orange Belgium NV"]

    pxs_wfs.to_file(f"{OUTPUT_DIR}/antennes_pxs.geojson", driver='GeoJSON')
    tnt_wfs.to_file(f"{OUTPUT_DIR}/antennes_tnt.geojson", driver='GeoJSON')
    org_wfs.to_file(f"{OUTPUT_DIR}/antennes_org.geojson", driver='GeoJSON')

    # get BIPT sites in bbox
    bipt_sites = get_bipt_sites_in_lambert_bbox(bbox[0], bbox[3], bbox[2], bbox[1])
    print_bipt_sites_statistics(bipt_sites)
    #bipt_sites = load_bipt_sites_from_json("data/bipt_leuven.json") # for testing
    pxs_bipt = bipt_sites[bipt_sites["Proximus"]==True] # all proximus sites
    tnt_bipt = bipt_sites[bipt_sites["Telenet"]==True] # all telenet sites
    org_bipt = bipt_sites[bipt_sites["Orange"]==True] # all orange sites
   
    operators = [{"name": "Proximus", "short": "pxs", "bipt": pxs_bipt, "wfs": pxs_wfs},
                {"name": "Telenet", "short": "tnt", "bipt": tnt_bipt, "wfs": tnt_wfs},
                {"name": "Orange", "short": "org", "bipt": org_bipt, "wfs": org_wfs}]

    for op in operators:
        print(f"-----------------------[ {op['name']} ]----------------------------------------")
        sites = get_features_for_sites(op["bipt"], op["wfs"])
        sites = download_attesten_for_features(sites, f"{DATA_DIR}/attesten_{op['short']}")
        parsed_attesten = parse_attesten_for_features(sites)
        sites_sector_list = get_sites_sectors_list(sites, parsed_attesten) # currently filters out non-L8 bands
        filename = f'{OUTPUT_DIR}/basestations_{op["short"]}.json'
        with open(filename, 'w') as outfile:
            json.dump(sites_sector_list, outfile, indent=4)
        print(f"--------Done! Output: {filename} ----------------------------")

    print(f"Done.")