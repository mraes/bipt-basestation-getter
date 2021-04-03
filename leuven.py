from BIPT_sites import get_bipt_sites_in_lambert_bbox, load_bipt_sites_from_json, get_features_for_sites, download_attesten_for_features, parse_attesten_for_features, get_sites_sectors_list
import geopandas as gpd
import json
import pdb

if __name__ == "__main__":

    dir = "leuven"
    # Get a GeoDataFrame (in Lambert 72) with BIPT sites within the bbox
    #sites = get_bipt_sites_in_lambert_bbox(bounds.left, bounds.top, bounds.right, bounds.bottom)
    sites = load_bipt_sites_from_json("data/bipt_leuven.json") # for testing

    pxs = sites[sites["Proximus"]==True] # all proximus sites
    tnt = sites[sites["Telenet"]==True] # all telenet sites
    org = sites[sites["Orange"]==True] # all orange sites
    print(f"PROXIMUS: {len(pxs.index)}, TELENET: {len(tnt.index)}, ORANGE: {len(org.index)} (Possibly with co-location)")
   
    print("PROXIMUS")
    print("---------------------------------------------------------------")
    wfs_pxs = gpd.read_file("data/wfs_pxs.geojson") # Extracted from QGIS
    # TODO: filter wfs_pxs with bounding box under investigation (possible in geopandas!)
    pxs_sites = get_features_for_sites(pxs, wfs_pxs)
    download_attesten_for_features(pxs_sites, f"{dir}/attesten_pxs")
    parsed_attesten = parse_attesten_for_features(pxs_sites)
    sites_sector_list = get_sites_sectors_list(pxs_sites, parsed_attesten) # currently filters out non-L8 bands
    with open(f'{dir}/basestations_pxs.json', 'w') as outfile:
        json.dump(sites_sector_list, outfile, indent=4)
    print("----------------------------------------------------------------")

    print("TELENET")
    print("---------------------------------------------------------------")
    wfs_tnt = gpd.read_file("data/wfs_tnt.geojson") # Extracted from QGIS
    # TODO: filter wfs_tnt with bounding box under investigation (possible in geopandas!)
    tnt_sites = get_features_for_sites(tnt, wfs_tnt)
    tnt_sites = download_attesten_for_features(tnt_sites, f"{dir}/attesten_tnt")
    parsed_attesten = parse_attesten_for_features(tnt_sites)
    sites_sector_list = get_sites_sectors_list(tnt_sites, parsed_attesten) # currently filters out non-L8 bands
    with open(f'{dir}/basestations_tnt.json', 'w') as outfile:
        json.dump(sites_sector_list, outfile, indent=4)