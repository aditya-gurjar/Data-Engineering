# Mapping between BRFSS and NHIS data 

## BRFSS
### Race Column:- _IMPRACE

    1 White, Non-Hispanic 
    2 Black, Non-Hispanic 
    3 Asian, Non-Hispanic 
    4 American Indian/Alaskan Native, Non-Hispanic
    5 Hispanic
    6 Other race, Non-Hispanic 

### Sex Column:- SEX
    1 Male
    2 Female
    9 Refused

### Age Column (Range):- _AGEG5YR

    1 Age 18 to 24 18 <= AGE <= 24
    2 Age 25 to 29
    3 Age 30 to 34
    4 Age 35 to 39
    5 Age 40 to 44
    6 Age 45 to 49
    7 Age 50 to 54
    8 Age 55 to 59
    9 Age 60 to 64
    10 Age 65 to 69
    11 Age 70 to 74
    12 Age 75 to 79
    13 Age 80 or older
    14 Don’t know/Refused/Missing

## NHIS
### Race Column:- MRACBPI2

    01 White  
    02 Black/African American  
    03 Indian (American) (includes Eskimo, Aleut)  
    06 Chinese  
    07 Filipino  
    12 Asian Indian  
    16 Other race*  
    17 Multiple race, no primary race selected  

### Sex Column:- SEX
    1 Male
    2 Female

### Age Column:- AGE_P
    00 Under 1 year  
    01-84 1-84 years  
    85 85+ years 

## HISPAN_I

    Hispanic subgroup detail  
    00 Multiple Hispanic  
    01 Puerto Rico  
    02 Mexican  
    03 Mexican-American  
    04 Cuban/Cuban American  
    05 Dominican (Republic)  
    06 Central or South American  
    07 Other Latin American, type not specified  
    08 Other Spanish  
    09 Hispanic/Latino/Spanish, non-specific type  
    10 Hispanic/Latino/Spanish, type refused  
    11 Hispanic/Latino/Spanish, type not ascertained  
    12 Not Hispanic/Spanish origin 


### Mapping (brrfs to nhis)
    1 -> 01
    2 -> 02
    3 -> 06, 07, 12
    4 -> 03
    5 -> ?
    6 -> 16

## DIBEV Diabetes

    Ever been told that you have diabetes
    1 Yes
    2 No
    3 Borderline or prediabetes
    7 Refused
    8 Not ascertained
    9 Don't know