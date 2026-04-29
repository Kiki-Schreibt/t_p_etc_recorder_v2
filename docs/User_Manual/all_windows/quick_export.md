site_name: Quick Export

When clicking *Quick Export* in the view menu the currently 
loaded, **complete** 
test will be exported to:
```  bash
C:\Users\%user_name%\T_p_ETC_recorder\Exports\%Sample_ID%
```
This can be used if you would like to further process the data to make publication ready plots in origin and so on. Will maybe be extended with originpro library to directly send the wanted data to origin and plot it as it is still annoying to dig through all the data columns...


For the export the limitations set in the main window 
for valid conductivity measurements
(e.g. minimum/maximum total temperature increase and total to characteristic time) 
will be applied and only conductivity values in the given boarders will be exported

Depending on the amount of data the export can last a few minutes.
After export The export folder will contain the following files:

1. %Sample_ID%_Capacity_Data.txt
   - An xy-column where x is the cycle number and y the released (negative numbers) 
     and absorbed (positive numbers) amount of hydrogen in mass percentage
2. %Sample_ID%_Conductivity_Data_all.txt
    - All measured conductivity values including average values over time. Some extra columns are exported as well. Just click the file and see the headings. 
3. %Sample_ID%_Conductivity_Data_cycles.txt
    - All conductivity data that is labeled with the *Is Cycle* checkbox during recording (see [main window](../main_window.md))
4. %Sample_ID%_Conductivity_Data_isotherms.txt
    - All conductivity data that is labeled with the *Is Isotherm* checkbox during recording  (see [main window](../main_window.md))
