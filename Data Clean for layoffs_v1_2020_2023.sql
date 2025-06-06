SELECT * 
FROM layoffs;

-- 1. Remove duplicate
-- 2. Standardize the Data
-- 3. null Values or blank values
-- 4. Remove Any Columns


/* 1. Remove Duplicate*/

# create staging table

create table layoffs_staging
like layoffs;

insert layoffs_staging
select *
from layoffs;

# identify duplicates by using cte and window function 

with duplicate_cte as 
(
	select *, 
		row_number() over(partition by 
			company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions 
						) as row_num
		from layoffs_staging
) 
select * from duplicate_cte
where row_num>=2;

# identify duplicates by using subquery and window function 

SELECT *
FROM (
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY 
               company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM layoffs_staging
) AS duplicate_sub
WHERE row_num >= 2;


# create another staging table as CTE does not allow updating

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert layoffs_staging2
SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY 
               company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM layoffs_staging;

delete  
from layoffs_staging2
where row_num>=2;

select *  
from layoffs_staging2
where row_num>=2;

# duplicates have been succesfully removed

/* 2. Standardizing Data*/

select *
from layoffs_staging2;

-- Standardizing Text Columns --

update layoffs_staging2
set 
	company=trim(company),
    location=trim(location),
    industry=trim(industry),
    stage=trim(stage),
    country=trim(country);

select distinct industry
from layoffs_staging2
order by 1;


select *
from layoffs_staging2
where industry like '%cry%'
order by industry desc;

update layoffs_staging2
set industry = 'Crypto'
where industry like '%cry%';

	/* Quick check the rest of text columns
		select distinct location/country
		from layoffs_staging2
		order by 1;
	*/

select distinct country
from layoffs_staging2
order by 1;

update layoffs_staging2
set country=trim(trailing '.' from country)
;

-- change the column date data type from text to date

# first, change the format 
select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set 
	date = str_to_date(`date`, '%m/%d/%Y')
;

# then, alter the data type
alter table layoffs_staging2
modify column `date` date;


/*3. null Values or blank values*/

select * 
from layoffs_staging2
where 
	total_laid_off is null 
    and 
    percentage_laid_off is null
; 

delete 
from layoffs_staging2
where 
	total_laid_off is null 
    and 
    percentage_laid_off is null
; 

select * 
from layoffs_staging2
where
	industry = ''
    or
    industry is null
;

	/* check all the rows from above
	select * 
	from layoffs_staging2
	where company like '%Air%'/'%Ball%'/'%carva%'/'%uul%';
	*/

# update rows accordingly 

select * 
from layoffs_staging2
where company like '%uul%';

update layoffs_staging2
set industry = 'Travel'
where company = 'Airbnb'
;

update layoffs_staging2
set industry = 'Transportation'
where company = 'Carvana'
;

update layoffs_staging2
set industry = 'Consumer'
where company = 'Juul'
;

	/* use conditions to simplify the above:
	UPDATE layoffs_staging2
	SET industry = CASE company
		WHEN 'Airbnb' THEN 'Travel'
		WHEN 'Carvana' THEN 'Transportation'
		WHEN 'Juul' THEN 'Consumer'
		ELSE industry
	END
	WHERE company IN ('Airbnb', 'Carvana', 'Juul');
	*/

	/* alternatively, use inner join to identify all the rows and update at once.  
    
	select t1.industry, t2.industry 
	from layoffs_staging2 t1
	join layoffs_staging2 t2
		on t1.company = t2.company
	where (t1.industry is null or t1.industry = '')
	and (t2.industry is not null and t2.industry !='');

	update layoffs_staging2 t1
	join layoffs_staging2 t2
		on t1.company = t2.company
	set t1.industry = t2.industry
	where (t1.industry is null or t1.industry = '')
	and (t2.industry is not null and t2.industry !='')
	;
	*/


select * 
from layoffs_staging2
where `date` is null; -- only one row returns, cannot run analysis with missing date, delete this row

delete 
from layoffs_staging2
where `date` is null 
;

alter table layoffs_staging2
drop column row_num
;


