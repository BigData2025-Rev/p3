## Combining Files using SAS

__Download and Install Altair SLC + Workbench__
 
Windows: https://drive.google.com/file/d/1eFjFqBQfjjjmgLEgxqES2esCAEn03pve/view?usp=drive_link

Mac: https://drive.google.com/file/d/1FA_wVov0PZEjxxyn4b2TK6cvFAD3V1RE/view?usp=drive_link

Install both SLC and Workbench. Once done, open SLC (will open in command line) and type `wps -personal` to activate your personal license (30 day trial). You should be able to open the workbench application. At the bottom, you will see "Local Server"--make sure it's running (it should start automatically if you activated your license properly).

__Download state files from FTP site__

In our case, 2010, from here: https://www2.census.gov/census_2010/redistricting_file--pl_94-171/

Once you have the zip file from a single state, unzip it into a folder. Your folder should now contain three .pl files - GeoHeaders, Part1, and Part2. Save this into an easily accessible directory as they will be needed later.

__Download the SAS program for merging all three files__

From the same website as above, scroll down to the bottom until you find `pl_all_3_2010.sas`. Save this into a separate folder named "sasprograms" or something in the same directory as your state files.

__Merging all 3 files using SAS__

Open `pl_all_3_2010.sas` in the workbench. Before running it, you'll need to change two variables:

* `libname xxx` Change this to the path of where you want your output to go, e.g. a new folder in the same directory called "censusdata" or something.
* `infile` This will need to be changed for each "data" block. Point the first one to your Part1 file, the second to your Part2 file, and the third one to the GeoHeaders file.

Once you have everything set up, run the program and the files will be automatically merged and output in your designated folder as a SAS dataset.

__Convert SAS dataset to CSV__

Create a new folder in the same directory--this will contain the SAS dataset you want to convert. Name it "toconvert" or similar. Place the dataset you made from the step above into this folder.

Now we need to create the SAS program to handle the conversion. Create a new file in workbench (File > New > Program) and paste the following code:

```libname convert 'C:\Users\Harish\Downloads\toconvert';

proc export data=convert.COMBINE           /**/
     outfile="C:\Users\Harish\Downloads\censusdata\Alaska2010.csv" /**/
     dbms=csv                                     /**/
     replace;                                     /**/
run;```

Replace `libname` path with the path on your machine to the folder containing the SAS dataset. The variable in `proc export data` will contain two parts--the libname you just created (pointing to the directory) and the file name of the dataset to convert (in my case, COMBINE). Replace `outfile` with the path you want to output to and the name of the .csv file. After running, the converted file will be in the folder.