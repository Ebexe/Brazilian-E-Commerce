/* =============================================================================
 PASO 02: NORMALIZACIÓN DEL ESQUEMA (Integridad Referencial)
=============================================================================
 Autor: Eberth Rojas Barbaran
 Fecha: 2025-12-31
 Descripción: Definir Primary Keys (PK) y Foreign Keys (FK) para el esquema public.
*/

-- ---------------------------------------------------------------------------
-- 1. DEFINICIÓN DE LLAVES PRIMARIAS (PK)
-- ---------------------------------------------------------------------------

-- Tabla Customers (Clientes)
ALTER TABLE public.customers ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);

-- Tabla Sellers (Vendedores)
ALTER TABLE public.sellers ADD CONSTRAINT pk_sellers PRIMARY KEY (seller_id);

-- Tabla Products (Productos)
ALTER TABLE public.products ADD CONSTRAINT pk_products PRIMARY KEY (product_id);

-- Tabla Orders (Pedidos)
ALTER TABLE public.orders ADD CONSTRAINT pk_orders PRIMARY KEY (order_id);

-- NOTA: geolocation tiene duplicados, reviews y payments no tienen ID único confiable 
-- para ser PK simple, así que las dejo como tablas transaccionales/referenciales.

-- ---------------------------------------------------------------------------
-- 2. DEFINICIÓN DE LLAVES FORÁNEAS (FK) - RELACIONES
-- ---------------------------------------------------------------------------

-- Relación: Un Pedido pertenece a un Cliente
ALTER TABLE public.orders 
ADD CONSTRAINT fk_orders_customer 
FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id);

-- Relación: Items (Detalle) -> Pedidos
ALTER TABLE public.items 
ADD CONSTRAINT fk_items_order 
FOREIGN KEY (order_id) REFERENCES public.orders(order_id);

-- Relación: Items -> Productos
ALTER TABLE public.items 
ADD CONSTRAINT fk_items_product 
FOREIGN KEY (product_id) REFERENCES public.products(product_id);

-- Relación: Items -> Vendedores
ALTER TABLE public.items 
ADD CONSTRAINT fk_items_seller 
FOREIGN KEY (seller_id) REFERENCES public.sellers(seller_id);

-- Relación: Pagos -> Pedidos
ALTER TABLE public.payments 
ADD CONSTRAINT fk_payments_order 
FOREIGN KEY (order_id) REFERENCES public.orders(order_id);

-- Relación: Reviews -> Pedidos
ALTER TABLE public.reviews 
ADD CONSTRAINT fk_reviews_order 
FOREIGN KEY (order_id) REFERENCES public.orders(order_id);

