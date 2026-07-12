## DESCRIPTION

*r.import.probav_lc* is a GRASS Addon that downloads and imports PROBA-V
land cover raster maps from [Copernicus Global Land Service: Land Cover
100m: collection 3: epoch 2019:
Globe](https://zenodo.org/record/3939050) for the current region.

The land cover maps of the years 2015 to 2019 are supported.

To avoid multiple downloads of the data, the user can specify a
**directory** where the data should be saved. If no **directory** is
specified, the data will be deleted after the import.

## REQUIREMENTS

### wget from Python3

```sh
pip3 install wget
```

### zenodo_get from Python3

```sh
pip3 install zenodo_get
```

## EXAMPLES

### Import 100m PROBA-V discrete classification and tree cover fraction map

```sh
r.import.probav_lc directory=probaV year=2019 \
  discrete_classification_output=discrete_classification_map \
  tree_coverfraction_output=tree_coverfraction_map
```

## SEE ALSO

*[r.import](https://grass.osgeo.org/grass-stable/manuals/r.import.html)*

## AUTHOR

Anika Weinmann, [mundialis](https://www.mundialis.de/), Germany
