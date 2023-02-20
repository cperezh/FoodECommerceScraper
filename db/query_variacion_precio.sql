with 
tablon as(
	select 
		pf.id_producto,
		pd.product,
		dd.date,
		pf.unit_price
	from producto_dia_fact pf
	join producto_dim pd on pd.id_producto = pf.id_producto
	join date_dim dd on dd.id_date = pf.id_date
),
ordered as(
	select *,
		row_number() over (partition by id_producto order by date asc) num
	from tablon
),
variacion as (
	select 
		o1.*,
		o2.num num2,
		o1.unit_price - o2.unit_price variacion,
		(round(cast((o1.unit_price / o2.unit_price) as numeric), 2)-1)*100 por_variacion
	from ordered o1
	left join ordered o2 on o1.id_producto = o2.id_producto
		and o1.num = o2.num+1
)
select * From variacion
where 1 = 1
--and date = (select max(date) from variacion)
--and variacion is not null and variacion != 0
order by id_producto, num asc

