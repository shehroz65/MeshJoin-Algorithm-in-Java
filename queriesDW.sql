#Q1

select sum(sales), product_name, supplier_name, quarters, months from fact
join product on fact.fk_product_ID = product.product_ID
join supplier on fact.fk_supplier_ID = supplier.supplier_ID 
join timing
GROUP BY supplier_ID, quarters, months;

#Q2

select supplier_name, product_name, sum(sales) from fact 
join product on fact.fk_product_ID = product.product_ID
join supplier on fact.fk_supplier_ID = supplier.supplier_ID
GROUP BY supplier_name, product_name;

#Q3

Select product_name, Weekday(fk_date) as DayName, Quantity from product 
join fact on product.product_id = fact.fk_product_id
where Weekday(fk_date) >= 5
Order By quantity desc limit 5;

#Q5

select product_name, sum(sales) as Sales	, 
Case 
	WHEN quarters <=2 THEN sum(quantity) else 0
END As q1,
case 
	when quarters>=3 then sum(quantity) else 0
end as q2
From fact join product on fact.fk_product_id = product.product_id
join timing on fact.FK_date=timing.timing_id
Group by product_name;

#Q6

SELECT PRODUCT_NAME, COUNT(PRODUCT_NAME)
FROM product
GROUP BY PRODUCT_NAME
HAVING COUNT(PRODUCT_NAME)>1;

#Q6 OLAP query shows that we have a Product Tomato that appears twice in our Products table
#different prices and Product_ID


#Q7
create view STOREANALYSIS_MV as (select STORE_ID, PRODUCT_ID, Sum(quantity)
from fact  natural join product, store
group by STORE_ID, PRODUCT_ID
);

create view STOREANALYSIS_MV as (select FK_STORE_ID, FK_PRODUCT_ID, Sum(QUANTITY) from fact 
natural join product natural join store group by FK_STORE_ID, FK_PRODUCT_ID);

select * from STOREANALYSIS_MV;


-- drop view STOREANALYSIS_MV;














