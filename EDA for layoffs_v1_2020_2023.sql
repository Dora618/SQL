-- Exploratory Data Analysis 


# explore attributes, observe the data range

select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select min(`date`), max(`date`)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off=1
order by total_laid_off DESC;

	/*
	The following queries use GROUP BY 
	to analyze the total number of layoffs across different attributes 
	(company, industry, country, and year), 
	helping identify where and when layoffs were most significant.
	*/

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2; 

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

	/*
	This query uses GROUP BY, CTE, and a window function 
	to calculate the monthly rolling total of total_laid_off.
	*/

with rolling as
(
select year(`date`) as `year`, month(`date`) as `month`, sum(total_laid_off) as num_laid_off
from layoffs_staging2
group by 1,2 
order by 1,2  asc
)
select 
concat(`year`,'-',`month`) as till_month, 
num_laid_off,
sum(num_laid_off) over(order by`year`, `month`) as rolling_total
from rolling ; 


/*
This query lists the top 5 companies with the highest number of layoffs for each year.
*/

with company_year as
(
select company, year(`date`) as years, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by company, years
), company_year_rank as
(select *, 
dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
order by ranking
)
select *
from company_year_rank
where ranking<=5
order by years;
