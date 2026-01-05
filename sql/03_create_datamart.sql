/* =============================================================================
 PASO 03: CREACIÓN DEL DATA MART (Star Schema)
=============================================================================
 Autor: Eberth Rojas Barbaran
 Descripción: Creación tablas DIM y FACT en el esquema 'analytics' para Power BI.
*/

-- 1. Crear esquema separado (Buenas Prácticas)
CREATE SCHEMA IF NOT EXISTS analytics;

-- ===========================================================================
-- DIMENSIÓN TIEMPO (DIM_TIME)
-- Extraemos fechas únicas y desglosamos año, mes, trimestre.
-- ===========================================================================
DROP TABLE IF EXISTS analytics.dim_time;

CREATE TABLE analytics.dim_time AS
SELECT DISTINCT 
    CAST(order_purchase_timestamp AS DATE) AS time_id,
    EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
    EXTRACT(MONTH FROM order_purchase_timestamp) AS month,
    EXTRACT(QUARTER FROM order_purchase_timestamp) AS quarter,
    TO_CHAR(order_purchase_timestamp, 'Day') AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM order_purchase_timestamp) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM public.orders
WHERE order_purchase_timestamp IS NOT NULL
ORDER BY 1;

ALTER TABLE analytics.dim_time ADD CONSTRAINT pk_dim_time PRIMARY KEY (time_id);


-- ===========================================================================
-- DIMENSIÓN CLIENTE (DIM_CUSTOMERS)
-- ===========================================================================
DROP TABLE IF EXISTS analytics.dim_customers;

CREATE TABLE analytics.dim_customers AS
SELECT 
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state
FROM public.customers c;

ALTER TABLE analytics.dim_customers ADD CONSTRAINT pk_dim_customers PRIMARY KEY (customer_id);


-- ===========================================================================
-- DIMENSIÓN PRODUCTO (DIM_PRODUCTS)
-- ===========================================================================
DROP TABLE IF EXISTS analytics.dim_products;

CREATE TABLE analytics.dim_products AS
SELECT 
    p.product_id,
    COALESCE(p.product_category_name, 'Sin Categoría') AS category_name,
    p.product_photos_qty,
    p.product_weight_g
FROM public.products p;

ALTER TABLE analytics.dim_products ADD CONSTRAINT pk_dim_products PRIMARY KEY (product_id);


-- ===========================================================================
-- TABLA DE HECHOS: VENTAS (FACT_ORDERS)
-- Cruce de Pedidos + Items + Métricas calculadas
-- ===========================================================================
DROP TABLE IF EXISTS analytics.fact_orders;

CREATE TABLE analytics.fact_orders AS
SELECT 
    -- Llaves foráneas (para conectar con dimensiones)
    o.order_id,
    o.customer_id,
    i.product_id,
    i.seller_id,
    CAST(o.order_purchase_timestamp AS DATE) AS time_id,
    
    -- Atributos
    o.order_status,
    
    -- Métricas Numéricas (KPIs)
    i.price,
    i.freight_value,
    (i.price + i.freight_value) AS total_amount,
    
    -- Cálculos de Tiempos Logísticos (En días)
    DATE_PART('day', o.order_delivered_customer_date - o.order_purchase_timestamp) AS days_to_deliver,
    DATE_PART('day', o.order_estimated_delivery_date - o.order_delivered_customer_date) AS delivery_diff_estimated,
    
    -- Bandera de Retraso (1 = Tarde, 0 = A tiempo)
    CASE 
        WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date THEN 1 
        ELSE 0 
    END AS is_late_delivery

FROM public.orders o
JOIN public.items i ON o.order_id = i.order_id
WHERE o.order_status != 'canceled';

-- Llave primaria subrogada para la tabla de hechos
ALTER TABLE analytics.fact_orders ADD COLUMN id SERIAL PRIMARY KEY;

