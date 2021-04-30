# bipt-basestation-getter
Gets basestation/antenna configuration for BIPT cellular basestation sites

## Example Usage
`python3 main.py -b 150698.5848,212583.605,154698.5848,216583.605 -o test`
Will get the known BIPT sites in the given bounding box, find the "conformiteitsattest" for those sites per operator, download them, parse them to json and then output them in the given directory `test`
