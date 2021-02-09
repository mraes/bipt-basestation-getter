import camelot
import pandas as pd
import sys
import pdb

columns = ['Antenna', 'Azimuth', 'Height', 'Width', 'Frequency', 'AGL', 'Power', 'ETilt', 'MTilt', 'HAperture', 'VAperture', 'Gain']

def format_dataframe(df):
    if df.iloc[0,-1] == "Technologie": # First (most recent) type of attest
        df.columns = df.iloc[0]
        df = df.drop(0)
        df = df[df.Technologie == "4G"]
        df = df.drop(columns=["NR", "Technologie"])
        df.columns = columns
        number_columns = ['Azimuth', 'Height', 'Width', 'Frequency', 'AGL', 'Power', 'MTilt', 'HAperture', 'VAperture', 'Gain']
        if(df.empty):
            return df # if the dataframe is empty because we throw out non-4G, return empty now
        df[number_columns] = df[number_columns].stack().str.replace(",", ".").unstack()
        df[number_columns] = df[number_columns].apply(pd.to_numeric)
    elif len(df.columns) == 13: # Second (slightly older) type of attest
        df.columns = df.iloc[0]
        df = df.drop(0)
        df = df.drop(columns=["NR"])
        df.columns = columns
        number_columns = ['Azimuth', 'Height', 'Width', 'Frequency', 'AGL', 'Power', 'MTilt', 'HAperture', 'VAperture', 'Gain']
        df[number_columns] = df[number_columns].stack().str.replace(",", ".").unstack()
        df[number_columns] = df[number_columns].apply(pd.to_numeric)

        # filter out 900 MHz (3G) & 2100 MHz (3G)
        df = df[abs(df.Frequency - 900) > 50]
        df = df[abs(df.Frequency - 2100) > 100]

        #TODO: FILTER OUT NON-4G freqs
    elif len(df.columns) == 14: # Third (even older) type of attest
        if df.iloc[0,0] == "Zendantennes": #double check
            df = df.drop([0,1])
            df = df.drop(columns=[0,8]) # drop the "Nr" column and the "Tilt" column, we get that from MTilt & Etilt
            df.columns = columns
            number_columns = ['Azimuth', 'Height', 'Width', 'Frequency', 'AGL', 'Power', 'MTilt', 'HAperture', 'VAperture', 'Gain']
            df[number_columns] = df[number_columns].apply(pd.to_numeric, errors='coerce')
            # filter out 900 MHz (3G) & 2100 MHz (3G)
            df = df[abs(df.Frequency - 900) > 50]
            df = df[abs(df.Frequency - 2100) > 100]
        else:
            pdb.set_trace()
            raise Exception("Conformiteitsattest not supported.")
    else:
        if(len(df.columns) > 10): # if it has many columns, it might be an attest, but not yet supported 
            pdb.set_trace()           
            raise Exception("CONFORMITEITSATTEST NOT SUPPORTED")
        else:
            # if its a small amount of columns, it doesn't contain antennas
            df = pd.DataFrame()
    return df

def parse_conformiteitsattest(pdfpath):
    # extracts BS details from a conformiteitsattest PDF downloaded from the site of the Flemish gov
    # Returns a pandas dataframe with the values
    tables = camelot.read_pdf(pdfpath, pages="2,3")
    df = pd.DataFrame(columns=columns)
    for table in tables:
        df = df.append(format_dataframe(table.df)).sort_values(by="Frequency", ascending=True)
    #df = df.drop([0,1])
    #df = df.drop(columns=[1])
    #df.columns = ['Antenna', 'Azimuth', 'Height', 'Width', 'Frequency', 'AGL', 'Power', 'Tilt', 'ETilt', 'MTilt', 'HAperture', 'VAperture', 'Gain']
    #df = df.apply(pd.to_numeric, errors='ignore')
    #df = df.reset_index(drop=True)
    #df['LTE'] = np.where(df['Antenna'].str.contains("L8"), True, False)
    """     conditions = [
    (df['Antenna'].str.contains("L8")),
    (df['Antenna'].str.contains("U9")),
    (df['Antenna'].str.contains("G9"))]
    choices = ["LTE", "UMTS", "GSM"]
    df['Tech'] = np.select(conditions, choices, default='N/A') """
    return df

if __name__ == "__main__":
    # execute only if run as a script
    df = parse_conformiteitsattest(sys.argv[1])
    print(df)