SELECT * FROM layoffs_v2.layoffs_v2;

-- create staging table 

create table layoffs_v2_staging
like layoffs_v2;

select *
from layoffs_v2_staging;

insert layoffs_v2_staging
select *
from layoffs_v2;


-- check dusplicates

with find_duplicate as 
(
	select *, row_number() 
	over(partition by 
			company, location, total_laid_off, 
            `date`, percentage_laid_off, industry, 
            'source', stage, funds_raised, country, date_added) as row_num
	from layoffs_v2_staging
) 
select * from find_duplicate
where row_num >1; 

-- modify date column from text type to date type

select `date`, 
str_to_date(`date`, '%m/%d/%Y')
from layoffs_v2_staging;

update layoffs_v2_staging
set 
`date`= str_to_date(`date`, '%m/%d/%Y')
;

alter table layoffs_v2_staging
modify column `date` date;


-- delect rows where percentage_laid_off=0 and total_laid_off=0 

delete
from layoffs_v2_staging
where percentage_laid_off=0 and total_laid_off=0;

-- droup clumns that are not relevant 

alter table layoffs_v2_staging
drop column `source`,
drop column date_added;

-- only keep data after 2023-03-11

delete 
from layoffs_v2_staging
where `date`<= '2023-03-11';

-- check if there is any data clean needs

select *
from layoffs_v2_staging
where industry is null or trim(industry)='';

select *
from layoffs_v2_staging
where company='Appsmith';

select distinct stage
from layoffs_v2_staging
order by 1;

select distinct company, trim(company)
from layoffs_v2_staging
order by 1;

update layoffs_v2_staging
set company = trim(company)
;

select * 
from layoffs_v2_staging
where `date` is null or trim(`date`)='';

/*
The below queries take two different ways list the top 5 companies 
with the highest number of layoffs for each year.
*/

-- nested 
with top5 as 
(
	with toplist as 
	(
		select company, year(`date`) as `year`,  sum(total_laid_off) as total_laid_off 
		from layoffs_v2_staging
		group by company, `year`
	)
	select *, dense_rank() over(partition by `year` order by total_laid_off desc) as ranking
	from toplist
)
select * 
from top5
where ranking<=5
;

-- not nested
with toplist as 
(
	select company, year(`date`) as `year`,  sum(total_laid_off) as total_laid_off 
	from layoffs_v2_staging
	group by company, `year`
), top5 as
(
	select *, dense_rank() over(partition by `year` order by total_laid_off desc) as ranking
	from toplist
)
select * 
from top5
where ranking<=5
;

/*
The below creates a new table (`top_laid_off`) and populates it with the top 5 ranked companies by total layoffs 
for each year. It combines records from the current dataset (`layoffs_v2_staging`) and a previous dataset 
(`layoffs_v1_2020_2023.layoffs_staging2`) into the same table for comparison and analysis.
*/

drop table if exists `top_laid_off`;
CREATE TABLE `top_laid_off` (
  `company` text,
  `years` int,
  `total_laid_off` text,
  `ranking` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from top_laid_off;

insert top_laid_off
select *
from (
	select*
    from(
		select *, dense_rank() over(partition by `years` order by total_laid_off desc) as ranking
		from (
			select company, year(`date`) as `years`,  sum(total_laid_off) as total_laid_off 
			from layoffs_v2_staging
			group by company, `years`
			) as toplist
		) as ranked
	where ranking <=5
) as top5;


insert top_laid_off 
select *
from (
	select *
	from (
		select *, 
		dense_rank() over(partition by years order by total_laid_off desc) as ranking
		from (
			select company, year(`date`) as years, sum(total_laid_off) as total_laid_off
			from layoffs_v1_2020_2023.layoffs_staging2
			group by company, years
			) as company_year
	) as ranked
	where ranking<=5
) as top5;

select *
from top_laid_off
order by ranking, years -- Meta and Intel ranked No.1 for two consecutive years





