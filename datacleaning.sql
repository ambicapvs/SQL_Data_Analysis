select *
from layoffs
;
#creating a duplicate dataset from raw data
drop table layoffs_staging;
#this creates the table with columns like layoffs 
create table layoffs_staging
like layoffs;
#now we need to enter the data from layoffs to layoffs staging
Insert layoffs_staging
select *
from layoffs;

#another way to create duplicate table from existing table

create table layoffs_staging
select *
from layoffs;

select * 
from layoffs_staging;
# now lets clean the data
#finding duplicates
#to find duplicates lets create row numbers
#put date column name in backticks only not single quotaions



with duplicate_cte as 
(
select *,
row_number()over(partition by company, location, industry,total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) 
as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num>1
;

select *
from layoffs_staging
where company='Hibob';

#Now we need to delete the duplicate columns

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int

  )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

#here we inserted all data with row num into new table
insert into layoffs_staging2
select *,
row_number()over(partition by company, location, industry,total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) 
as row_num
from layoffs_staging;
  
select *
from layoffs_staging2
;
  
delete
from layoffs_staging2
where row_num>1;

############step 2 -Standardization

select *
from layoffs_staging2
;
#looking for any issues inthe columns individually and correcting if any issues are found
select distinct company, TRIM( company)
from layoffs_staging2
order by 1
;

update layoffs_staging2
set company = TRIM( company);

select distinct location
from layoffs_staging2
order by 1
;

select distinct location
from layoffs_staging2
order by 1
;


select distinct industry
from layoffs_staging2
order by 1
;
# we found labelling issues like crpto, crypto currency etc. so we are correcting
select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = "Crypto"
where industry like 'crypto%';

select *
from layoffs_staging2
where industry like 'fin-%';

#now country column

select distinct country
from layoffs_staging2
order by 1
;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United states%';



select distinct country
from layoffs_staging2
order by 1;

Select `date`,
str_to_date(`date`,'%m/%d/%Y') as newdate
from layoffs_staging2;
#this is not working
update layoffs_staging2
set `date`=str_to_date(`date`,'%m/%d/%Y');

# if we try to change the date format directly from text to date it wont work
# so first change the format of the date and then try changing the datatype
ALTER TABLE layoffs_staging2
modify column `date` date;
#not working till here

#### step 3- null /missing values
#in my dataset nulls have changed ibto none


select *
from layoffs_staging2
where industry ='none' or industry is null ;

select *
from layoffs_staging2
where company like 'bally%'
;

#now we need to update the blank data that we know can be updated 
#for that we are using a self join

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company and t1.location=t2.location
where t1.industry is null and t2.industry is not null;

#to update the blanks with values 
#first lets convert all blanks into nulls

SET SQL_SAFE_UPDATES = 0;

update layoffs_staging2
set industry=null
where industry='';

# now populate the null values with values

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company and t1.location=t2.location
set t1.industry =t2.industry 
where t1.industry is null and t2.industry is not null;


####lets check next column null values
##i changed my none vales to null
select *
from layoffs_staging2
where percentage_laid_off is null
;

update layoffs_staging2
set percentage_laid_off=null
where percentage_laid_off='none';

select *
from layoffs_staging2
where percentage_laid_off is null
and total_laid_off is null
;

#now we cannot populate this data so lets delete 
delete
from layoffs_staging2
where percentage_laid_off is null
and total_laid_off is null
;

select *
from layoffs_staging2;

#drop row_num column

alter table layoffs_staging2
drop column row_num;

#Now our data is ready to use for further analysis/visualization
select *
from layoffs_staging2;



















