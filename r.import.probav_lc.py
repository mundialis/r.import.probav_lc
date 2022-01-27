#!/usr/bin/env python3
############################################################################
#
# MODULE:       r.import.probav_lc
# AUTHOR(S):    Anika Weinmann
# PURPOSE:      Downloads and imports PROBA-V land cover raster maps from
#               https://zenodo.org/record/3939050
# COPYRIGHT:   (C) 2021-2022 by mundialis GmbH & Co. KG and the GRASS Development Team
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
############################################################################

# %module
# % description: Downloads and imports PROBA-V land cover raster maps from https://zenodo.org/record/3939050.
# % keyword: raster
# % keyword: import
# % keyword: PROBA-V
# % keyword: satellite
# %end

# %option G_OPT_R_OUTPUT
# % key: bare_coverfraction_output
# % required: no
# % label: Output raster map name for bare cover fraction map
# %end

# %option G_OPT_R_OUTPUT
# % key: builtup_coverfraction_output
# % required: no
# % label: Output raster map name for built-up cover fraction map
# %end

# %option G_OPT_R_OUTPUT
# % key: crops_coverfraction_output
# % required: no
# % label: Output raster map name for crops cover fraction map
# %end

# %option G_OPT_R_OUTPUT
# % key: change_confidence_output
# % required: no
# % label: Output raster map name for change confidence map
# %end

# %option G_OPT_R_OUTPUT
# % key: data_density_indicator_output
# % required: no
# % label: Output raster map name for data density indicator map
# %end

# %option G_OPT_R_OUTPUT
# % key: discrete_classification_output
# % required: no
# % label: Output raster map name for discrete classification map
# %end

# %option G_OPT_R_OUTPUT
# % key: discrete_classification_proba_output
# % required: no
# % label: Output raster map name for discrete classification proba map
# %end

# %option G_OPT_R_OUTPUT
# % key: forest_type_output
# % required: no
# % label: Output raster map name for forest type map
# %end

# %option G_OPT_R_OUTPUT
# % key: grass_coverfraction_output
# % required: no
# % label: Output raster map name for grass cover fraction map
# %end

# %option G_OPT_R_OUTPUT
# % key: moss_lichen_coverfraction_output
# % required: no
# % label: Output raster map name for moss lichen cover fraction map
# %end

# %option G_OPT_R_OUTPUT
# % key: permanent_water_coverfraction_output
# % required: no
# % label: Output raster map name for permanent water cover fraction map
# %end

# %option G_OPT_R_OUTPUT
# % key: seasonal_water_coverfraction_output
# % required: no
# % label: Output raster map name for seasonal water cover fraction map
# %end

# %option G_OPT_R_OUTPUT
# % key: shrub_coverfraction_output
# % required: no
# % label: Output raster map name for shrub cover fraction map
# %end

# %option G_OPT_R_OUTPUT
# % key: snow_coverfraction_output
# % required: no
# % label: Output raster map name for snow cover fraction map
# %end

# %option G_OPT_R_OUTPUT
# % key: tree_coverfraction_output
# % required: no
# % label: Output raster map name for tree cover fraction map
# %end

# %option G_OPT_M_DIR
# % key: directory
# % required: no
# % multiple: no
# % label: Directory path where to download the data. If not set the data will downloaded to temporary directory and removed after the import.
# %end

# %option
# % key: year
# % required: yes
# % multiple: no
# % label: Year of the data
# % options: 2015-2019
# % answer: 2019
# %end

# %rules
# % requires:discrete_classification_output,bare_coverfraction_output,builtup_coverfraction_output,crops_coverfraction_output,change_confidence_output,data_density_indicator_output,discrete_classification_output,discrete_classification_proba_output,forest_type_output,grass_coverfraction_output,moss_lichen_coverfraction_output,permanent_water_coverfraction_output,seasonal_water_coverfraction_output,shrub_coverfraction_output,snow_coverfraction_output,tree_coverfraction_output
# %end

import atexit
import os
import pickle
import psutil
import shutil
import sys
import wget

import grass.script as grass
from osgeo.gdal import Warp
from zenodo_get.zget import zenodo_get

rm_folders = []
download_dir = None
if "GDAL_CACHEMAX" in os.environ:
    GDAL_CACHEMAX = os.environ["GDAL_CACHEMAX"]
else:
    GDAL_CACHEMAX = None
if "COMPRESS_OVERVIEW" in os.environ:
    COMPRESS_OVERVIEW = os.environ["COMPRESS_OVERVIEW"]
else:
    COMPRESS_OVERVIEW = None

records = {
    "2015": "3939038",
    "2016": "3518026",
    "2017": "3518036",
    "2018": "3518038",
    "2019": "3939050",
}


def cleanup():
    grass.message(_("Cleaning up.."))
    for folder in rm_folders:
        if os.path.isdir(folder):
            shutil.rmtree(folder)
    if COMPRESS_OVERVIEW is not None:
        os.environ["COMPRESS_OVERVIEW"] = COMPRESS_OVERVIEW
    elif "GDAL_CACHEMAX" in os.environ:
        del os.environ["COMPRESS_OVERVIEW"]
    if GDAL_CACHEMAX is not None:
        os.environ["GDAL_CACHEMAX"] = GDAL_CACHEMAX
    elif "COMPRESS_OVERVIEW" in os.environ:
        del os.environ["GDAL_CACHEMAX"]


def categories_for_discrete_classification():
    # https://zenodo.org/record/3938963 (s. 28/29)
    discrete_classification_coding = {
        "0": "No inputdata available",
        "111": "Closed forest, evergreen needle leaf",
        "113": "Closed forest, deciduous needle leaf",
        "112": "Closed forest, evergreen, broad leaf",
        "114": "Closed forest, deciduous broad leaf",
        "115": "Closed forest, mixed",
        "116": "Closed forest, unknown",
        "121": "Open forest, evergreen needle leaf",
        "123": "Open forest, deciduous needle leaf",
        "122": "Open forest, evergreen broad leaf",
        "124": "Open forest, deciduous broad leaf",
        "125": "Open forest, mixed",
        "126": "Open forest, unknown",
        "20": "Shrubs",
        "30": "Herbaceous vegetation",
        "90": "Herbaceous wetland",
        "100": "Moss and lichen",
        "60": "Bare / sparse vegetation",
        "40": "Cultivated and managed vegetation/agriculture (cropland)",
        "50": "Urban/ built up",
        "70": "Snow and Ice",
        "80": "Permanent water bodies",
        "200": "Open sea",
    }
    # category
    category_text = ""
    for class_num, class_text in discrete_classification_coding.items():
        category_text += "%s|%s\n" % (class_num, class_text)
    cat_proc = grass.feed_command(
        "r.category",
        map=options["discrete_classification_output"],
        rules="-",
        separator="pipe",
    )
    cat_proc.stdin.write(category_text.encode())
    cat_proc.stdin.close()
    cat_proc.wait()


def get_filenames(allfilenames):
    filename_list = dict()
    # rastername_list = dict()
    if options["discrete_classification_output"]:
        for entry in allfilenames:
            if "discrete-classification-map" in entry.lower():
                filename_list[entry] = options["discrete_classification_output"]
    if options["bare_coverfraction_output"]:
        for entry in allfilenames:
            if "bare-coverfraction" in entry.lower():
                filename_list[entry] = options["bare_coverfraction_output"]
    if options["builtup_coverfraction_output"]:
        for entry in allfilenames:
            if "builtup-coverfraction" in entry.lower():
                filename_list[entry] = options["builtup_coverfraction_output"]
    if options["crops_coverfraction_output"]:
        for entry in allfilenames:
            if "crops-coverfraction" in entry.lower():
                filename_list[entry] = options["crops_coverfraction_output"]
    if options["change_confidence_output"]:
        for entry in allfilenames:
            if "change-confidence" in entry.lower():
                filename_list[entry] = options["change_confidence_output"]
    if options["data_density_indicator_output"]:
        for entry in allfilenames:
            if "datadensityindicator" in entry.lower():
                filename_list[entry] = options["data_density_indicator_output"]
    if options["discrete_classification_proba_output"]:
        for entry in allfilenames:
            if "discrete-classification-proba" in entry.lower():
                filename_list[entry] = options["discrete_classification_proba_output"]
    if options["forest_type_output"]:
        for entry in allfilenames:
            if "forest-type" in entry.lower():
                filename_list[entry] = options["forest_type_output"]
    if options["grass_coverfraction_output"]:
        for entry in allfilenames:
            if "grass-coverfraction" in entry.lower():
                filename_list[entry] = options["grass_coverfraction_output"]
    if options["moss_lichen_coverfraction_output"]:
        for entry in allfilenames:
            if "mosslichen-coverfraction" in entry.lower():
                filename_list[entry] = options["moss_lichen_coverfraction_output"]
    if options["permanent_water_coverfraction_output"]:
        for entry in allfilenames:
            if "permanentwater-coverfraction" in entry.lower():
                filename_list[entry] = options["permanent_water_coverfraction_output"]
    if options["seasonal_water_coverfraction_output"]:
        for entry in allfilenames:
            if "seasonalwater-coverfraction" in entry.lower():
                filename_list[entry] = options["seasonal_water_coverfraction_output"]
    if options["shrub_coverfraction_output"]:
        for entry in allfilenames:
            if "shrub-coverfraction" in entry.lower():
                filename_list[entry] = options["shrub_coverfraction_output"]
    if options["snow_coverfraction_output"]:
        for entry in allfilenames:
            if "snow-coverfraction" in entry.lower():
                filename_list[entry] = options["snow_coverfraction_output"]
    if options["tree_coverfraction_output"]:
        for entry in allfilenames:
            if "tree-coverfraction" in entry.lower():
                filename_list[entry] = options["tree_coverfraction_output"]
    return filename_list


def main():

    global rm_folders, download_dir, COMPRESS_OVERVIEW, GDAL_CACHEMAX

    pid = str(os.getpid())

    # year and record
    year = int(options["year"])
    record = records[options["year"]]

    # request server
    tmp_dir = grass.tempdir()
    rm_folders.append(tmp_dir)
    zenodo_get(["-r", record, "-w", "urls_%d_%s.txt" % (year, pid), "-o", tmp_dir])

    # get urls
    with open(os.path.join(tmp_dir, "urls_%d_%s.txt" % (year, pid))) as file:
        urls = {
            os.path.basename(x): x
            for x in file.read().split("\n")
            if x != "" and x.endswith(".tif")
        }

    # get filenames
    filenames = get_filenames(urls)

    # md5sum
    with open(os.path.join(tmp_dir, "md5sums.txt")) as file:
        md5sums = {
            x.split(" ")[-1]: x.split(" ")[0]
            for x in file.read().split("\n")
            if x != ""
        }

    # download directory
    files_to_download = dict()
    old_md5sums = dict()
    if options["directory"]:
        dir = options["directory"]
        download_dir = os.path.join(dir, str(year))
        if not os.path.isdir(download_dir):
            os.makedirs(download_dir)
            files_to_download = {
                filename: os.path.join(download_dir, filename) for filename in filenames
            }
        else:
            md5file = os.path.join(download_dir, "md5sums.pkl")
            if os.path.isfile(md5file):
                with open(md5file, "rb") as pickle_in:
                    old_md5sums = pickle.load(pickle_in)
            else:
                grass.warning(
                    _(
                        f"No old md5sums file found ({md5file}). "
                        + "All tifs will be downloaded."
                    )
                )
            for filename in filenames:
                tif_path = os.path.join(download_dir, filename)
                if os.path.isfile(tif_path):
                    if (
                        filename in old_md5sums
                        and md5sums[filename] != old_md5sums[filename]
                    ):
                        files_to_download[filename] = tif_path
                    elif filename not in old_md5sums:
                        files_to_download[filename] = tif_path
                else:
                    files_to_download[filename] = tif_path
    else:
        download_dir = grass.tempdir()
        rm_folders.append(download_dir)
        files_to_download = {
            filename: os.path.join(download_dir, filename) for filename in filenames
        }
    # download data
    for file, path in files_to_download.items():
        grass.message(_("Downloading %s ...") % file)
        downloaded_file = wget.download(urls[file], out=path)
        if os.path.isfile(downloaded_file):
            old_md5sums[file] = md5sums[file]

    # save new md5sums
    if len(files_to_download) > 0 and options["directory"]:
        md5file = os.path.join(download_dir, "md5sums.pkl")
        with open(md5file, "wb") as f:
            pickle.dump(old_md5sums, f, pickle.HIGHEST_PROTOCOL)

    # gdalwarp for reprojection
    for file, out_name in filenames.items():
        inpath = os.path.join(download_dir, file)
        free_memory = psutil.virtual_memory().free / 1024.0 ** 2  # bytes in MB
        os.environ["GDAL_CACHEMAX"] = str(0.8 * free_memory)
        os.environ["COMPRESS_OVERVIEW"] = "LZW"
        kwargs = dict()
        outpath = os.path.join(tmp_dir, "%s_%s.tif" % (file.replace(".tif", ""), pid))
        region = grass.parse_command("g.region", flags="pagu")
        proj = grass.parse_command("g.proj", flags="g")
        if "epsg" in proj:
            epsg = proj["epsg"]
        else:
            epsg = proj["srid"].split("EPSG:")[1]
        kwargs["dstSRS"] = "EPSG:{}".format(epsg)
        kwargs["srcSRS"] = "EPSG:4326"
        kwargs["outputBoundsSRS"] = kwargs["dstSRS"]
        ew_ints = [float(region["e"]), float(region["w"])]
        ns_ints = [float(region["n"]), float(region["s"])]
        kwargs["outputBounds"] = (
            min(ew_ints),
            min(ns_ints),
            max(ew_ints),
            max(ns_ints),
        )
        if proj["unit"].lower() == "meter":
            kwargs["xRes"] = 100
            kwargs["yRes"] = 100
            kwargs["targetAlignedPixels"] = True
        try:
            Warp(
                outpath,
                inpath,
                resampleAlg="near",
                format="GTiff",
                overviewLevel=5,
                creationOptions=["TILED=YES", "COMPRESS=LZW"],
                **kwargs,
            )
        except Exception:
            grass.fatal(_("Reprojection of Scene %s failed." % inpath))
        if not os.path.isfile(outpath):
            grass.fatal(_("Reprojection of Scene %s failed." % inpath))

        # import
        grass.run_command("r.import", input=outpath, output=out_name)
        grass.message(_("Imported <%s>") % (filenames[file]))

    # category for discrete classification:
    if options["discrete_classification_output"]:
        categories_for_discrete_classification()

    return 0


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    sys.exit(main())
