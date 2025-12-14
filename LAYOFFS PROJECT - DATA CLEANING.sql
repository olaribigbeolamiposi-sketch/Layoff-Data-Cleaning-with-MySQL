-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT*
FROM layoffs;

CREATE TABLE layoffs_stagging
LIKE layoffs;

SELECT *
FROM layoffs_stagging;

INSERT INTO layoffs_stagging
SELECT*
FROM layoffs;

SELECT company, industry, total_laid_off,`date`, 
row_number() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS Row_num
from layoffs_stagging;

SELECT*
FROM (SELECT company, industry, total_laid_off,`date`, 
row_number() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS Row_num
from layoffs_stagging
) duplicates
WHERE Row_num > 1
;

SELECT *
FROM layoffs_stagging
WHERE company = 'Better.com';

WITH DELETE_CTE AS
(
SELECT*
FROM 
(SELECT company, industry, total_laid_off,`date`, 
row_number() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS Row_num
from layoffs_stagging
) duplicates
WHERE Row_num > 1
)
DELETE
FROM DELETE_CTE;

ALTER TABLE layoffs_stagging ADD Row_num INT;

CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `Row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_stagging2;

INSERT INTO layoffs_stagging2
SELECT *, row_number() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) AS Row_num
from layoffs_stagging;

INSERT INTO `layoffs_stagging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_stagging;
        
DELETE 
FROM layoffs_stagging2
WHERE Row_num >1
;

SELECT* 
FROM layoffs_stagging2
WHERE Row_num >1
;

SELECT distinct company
FROM layoffs_stagging2
ORDER BY 1
;

SELECT*
FROM layoffs_stagging2
WHERE company like 'impossible%';

UPDATE layoffs_stagging2
SET company = 'Impossible Foods'
WHERE company like 'impossible%';

SELECT distinct industry
FROM layoffs_stagging2
ORDER BY 1
;

SELECT *
FROM layoffs_stagging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoffs_stagging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_stagging2
WHERE company LIKE 'airbnb%';

UPDATE layoffs_stagging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_stagging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

UPDATE layoffs_stagging2 T1
JOIN layoffs_stagging2 T2
ON T1.company = T2.company
SET T1.industry = T2.industry
WHERE T1.industry IS NULL
AND T2.industry IS NOT NULL;

SELECT distinct industry
FROM layoffs_stagging2
order by 1;

SELECT*
FROM layoffs_stagging2
WHERE industry like 'crypto%';

UPDATE layoffs_stagging2
SET industry = 'Crypto'
WHERE industry like 'Crypto%';

SELECT distinct industry
FROM layoffs_stagging2
order by 1;

SELECT distinct country
FROM layoffs_stagging2
order by 1;

UPDATE layoffs_stagging2
SET country = TRIM(TRAILING '.' FROM country);

SELECT*
FROM layoffs_stagging2;

UPDATE layoffs_stagging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_stagging2
MODIFY COLUMN `date` DATE;


SELECT*
FROM layoffs_stagging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE FROM layoffs_stagging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT*
FROM layoffs_stagging2;

ALTER TABLE layoffs_stagging2
DROP COLUMN Row_num;

SELECT*
FROM layoffs_stagging2;