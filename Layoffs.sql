-- SQL Project - Data Cleaning 
-- I downloaded my data from: https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- First thing I want to do is to create a schema and import my downloaded data

select* 
from layoffs;

-- To start cleaning the data, I have to create a different table where I can apply all my changes to
-- I don't want to apply changes using the main data

create table layoff_staging
select* 
from layoffs;

select*
from layoff_staging;

-- To start cleaning my data, I'm going to start with removing duplicates

select*, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) as row_num
from layoff_staging;

-- I am going too need a cte to check row_num that is > 1 since row_num was not part of the table to begin with

with cte as 
(select*, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) as row_num
from layoff_staging)
select*
from cte 
where row_num > 1;

-- Then I'll create a table so the row_num can be part of the table and I can delete the duplicates

create table layoff_staging2
with cte as 
(select*, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) as row_num
from layoff_staging)
select*
from cte;

select*
from layoff_staging2
where row_num > 1;

delete
from layoff_staging2
where row_num > 1;

-- Now that duplicate has been deleted, I move on to standardizing the data
# I'm going to go through each columns to try and correct every error I can see

select*
from layoff_staging2;

select distinct company
from layoff_staging2
order by 1;

-- If you notice, the first two rows under company needs to trimmed 

update layoff_staging2
set company = trim(company);

select*
from layoff_staging2;

select distinct location
from layoff_staging2
order by 1;

-- so I don't seem to notice any error under location, moving on to the next column

select*
from layoff_staging2;

select distinct industry
from layoff_staging2
order by 1;

-- so under industry, there is a blank and a null
-- there is also an error in crypto and crypto currency since they are both the same
# let's address the crypto issue first before going back to the null and blank

update layoff_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry
from layoff_staging2
order by 1;

# I'll be changing the blank to a null so it will be easier to work with

update layoff_staging2
set industry = null
where industry = '';

select distinct industry
from layoff_staging2
order by 1;

# now let's see why there is a null there 

select*
from layoff_staging2
where industry is null;

# Now let's see if there are other rows with the same company name and location
# If that is the case then I will populate the data, cause it's only normal for the industry to be the same

select*
from layoff_staging2
where company = 'Airbnb';

select*
from layoff_staging2
where company = "Bally's Interactive";

# there is only one of Bally's Interactive, so I won't be able to populate that

select*
from layoff_staging2
where company = 'Carvana';

select*
from layoff_staging2
where company = 'Juul';

-- I'm able to find that Airbnb, Carvana and Juul have other rows with the same company and location
-- only Bally's Interactive don't have other rows that's similar, so I can't populate that

select*
from layoff_staging2 as t1
join layoff_staging2 as t2
on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;

update 
layoff_staging2 as t1
join layoff_staging2 as t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- let'see how that worked out

select*
from layoff_staging2
where industry is null;

select*
from layoff_staging2
where company = 'Airbnb';

-- so the nulls have been populated successfully
# now moving to the next column

select*
from layoff_staging2;

# can't do anything about total_laid_off and percentage_laid_off cause there are numbers, so I move to date
-- the date format is wrong, so I have to change it from sring to date

select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoff_staging2;

update layoff_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoff_staging2
modify column `date` date;

select*
from layoff_staging2;

-- now I'm done with the date, I move on to stage

select distinct stage
from layoff_staging2
order by 1;

# so I notice a null, I'm just going to check if I can populate this 

select*
from layoff_staging2
where stage is null;

select*
from layoff_staging2
where company = 'Advata';

# I checked for companies that is the same with the nulls in stage so I can populate them
# but there is only one of each so I'll be leaving the column stage untouched 
# I'll be moving on to country

select*
from layoff_staging2;

select distinct country
from layoff_staging2
order by 1;

update layoff_staging2
set country = 'United States'
where country like 'United States%';

# That's all for country, I'll be leaving funds_raised_millions untouched
# Now I'm going to remove unneccessary columns 

alter table layoff_staging2
drop column row_num;

select*
from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

# I'll be deleting the columns where total_laid_off and percentage_laid_off is null 
# The point of this data is to see the numbers of laid off by companies 
# the purpose is defeated if total_laid_off and percentage_laid_off are both nulls

delete
from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

select*
from layoff_staging2;

-- I'm done with cleaning of the data
-- Let's explore the data a little bit
-- Here I am just going to explore the data and find trends or patterns or anything interesting

# maximum of total_laid_off
select max(total_laid_off)
from layoff_staging2;

# total_laid_off per company from max to min
select company, max(total_laid_off)
from layoff_staging2
group by company
order by 2 desc;

# max and min percentage laid off
select max(percentage_laid_off), min(percentage_laid_off)
from layoff_staging2
where percentage_laid_off is not null;

# companies with 1, which is basically 100% of their company
select*
from layoff_staging2
where percentage_laid_off = 1;

# Let's see how big these companies with 1 were
select*
from layoff_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

# companies with the biggest single layoffs
select company, max(total_laid_off)
from layoff_staging2
group by company
order by 2 desc;

#companies with the most total layoffs
select company, sum(total_laid_off)
from layoff_staging2
group by company
order by 2 desc;

#companies with the most total layoffs by location
select location, sum(total_laid_off)
from layoff_staging2
group by location
order by 2 desc;

#country with the most total layoffs
select country, sum(total_laid_off)
from layoff_staging2
group by country
order by 2 desc;

# year and total layoffs 
select year(`date`), sum(total_laid_off)
from layoff_staging2
group by  year(`date`)
order by 1 desc;

#industry with the most total layoffs
select industry, sum(total_laid_off)
from layoff_staging2
group by industry
order by 2 desc;

#stage with the most total layoffs
select stage, sum(total_laid_off)
from layoff_staging2
group by stage
order by 2 desc;

# first 3 companies with the highest total layoffs from 2020-2023
with company_year as
(select company, year(`date`) as years, sum(total_laid_off) as total_laid_off
from layoff_staging2
group by company ,year(`date`)),
company_year_ranking as
(select company, years, total_laid_off, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year)
select company, years, total_laid_off, ranking
from company_year_ranking
where ranking <= 3
and total_laid_off is not null
order by years desc;

# Rolling Total of Layoffs Per Month
select substring(`date`, 1,7) as `date`, sum(total_laid_off) as total_laid_off
from layoff_staging2
group by substring(`date`, 1,7)
order by 1;

with date_cte as 
(select substring(`date`, 1,7) as `date`, sum(total_laid_off) as total_laid_off
from layoff_staging2
group by substring(`date`, 1,7)
order by 1)
select `date`, sum(total_laid_off) over(order by `date`) as rolling_total
from date_cte
