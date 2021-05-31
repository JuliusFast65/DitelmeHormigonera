import string
import traceback
import pymssql

P = "12345"
U = "GestecNET"
IP = "localhost"

class DatabaseFetcher:

    @staticmethod
    def open_connection():
        conn = pymssql.connect(server='localhost', user=U, password=P, database=U)
        return conn

    @staticmethod
    def retrieve_orders(date_str:string,last_order_code=None):

        connection = DatabaseFetcher.open_connection()

        try:
            cursor = connection.cursor(as_dict=True)
            sql = DatabaseFetcher.build_query_orders(last_order_code=last_order_code)

            if last_order_code is None:
                print("FETCHING ALL ORDERS IN RANGE")
                cursor.execute(sql, current_date=date_str)
            else:
                print("FETCHING ONLY NEW", last_order_code)
                cursor.execute(sql, last_order_code=last_order_code)

            formatted_orders = []
            data = cursor.fetchall()

            print(data)
            index = 0
            last_order_code = None

            while index < len(data):

                order = data[index]
                order = {k.casefold(): v for k, v in order.items()}

                if index == 0:
                    last_order_code = order['order_code']

                item_index = 1
                new_index = index + 1

                order_products = [DatabaseFetcher._get_order_product(order, item_index)]

                while new_index < len(data):
                    next_order = data[new_index]
                    next_order = {k.casefold(): v for k, v in next_order.items()}

                    if next_order['order_code'] != order['order_code']:
                        break
                    else:
                        order_products.append(DatabaseFetcher._get_order_product(next_order,item_index))

                        item_index += 1
                        index += 1

                    new_index += 1

                order['order_products'] = order_products

                formatted_orders.append(order)
                index += 1

            cursor.close()

            print(formatted_orders)
            print("LAST UP DATE",last_order_code)

            return formatted_orders

        except Exception as e:
            traceback.print_exc()
        finally:
            connection.close()

    @staticmethod
    def _get_order_product(order:dict,product_index:int):
        order_product = {
            'delivery_type' : order.get('delivery_type',2),
            'price': order['price'],
            'item_number': product_index,
            'product_type': order['product_type'],
            'quantity': order['quantity'],
            'product_code': order['product_code'],
            'product_name': order['product_name']
        }
        return order_product

    # raise exception da conexao do banco
    @staticmethod
    def retrieve_tickets(date_str: string, last_updated_date=None):

        connection = DatabaseFetcher.open_connection()

        try:
            cursor = connection.cursor()
            sql = DatabaseFetcher.build_query(last_updated_date=last_updated_date)

            if last_updated_date is None:
                print("FETCHING ALL IN RANGE")
                cursor.execute(sql, current_date=date_str)
            else:
                print("FETCHING ONLY NEW", last_updated_date)
                cursor.execute(sql, last_updated_date=last_updated_date)

            cursor.rowfactory = DatabaseFetcher.dict_row_factory(cursor)

            formatted_requests = []
            data = cursor.fetchall()

            print(data)
            index = 0

            for req in data:

                req['START'] = req.pop('STARTX')

                if index == 0:
                    last_updated_date = req['START']

                formatted_requests.append(req)
                index += 1

            cursor.close()

            print("LAST UP DATE",last_updated_date)

            return formatted_requests,last_updated_date

        except Exception as e:
            traceback.print_exc()
        finally:
            connection.close()

    @staticmethod
    def get_row_value(row: dict, key: string, type='string'):
        val = row.get(key, None)

        if val is not None and type == 'string':
            val = str(val)

        return val

    @staticmethod
    def build_query(last_updated_date=None) -> string:

        sql = "SELECT \
            OPR.NRO_ORDEM_PRODUCAO_REMESSA as DELIVERYCODE, \
            TO_CHAR(OPR.data_lancamento,'YYYY-MM-DD HH24:MI:SS') as STARTX, \
            OPR.cod_agente_motorista as DRIVERCODE, \
            OPR.nome_motorista as DRIVERNAME, \
            OPR.cod_equipamento as TRUCKCODE, \
            OPR.cod_item as PRODUCTCODE, \
            I.descricao as PRODUCTNAME, \
            OPR.equipamento_lacre as SEAL,\
            P.NRO_PEDIDO as ORDER_CODE, \
            OPR.cod_empresa as PLANTCODE,\
            OPR.qtd as VOLUME,\
            A.COD_AGENTE as CUSTOMERCODE,\
            A.NOME as CUSTOMERNAME,\
            AE.COD_AGENTE as ADDRESSCODE,\
            AE.NOME as ADDRESSNAME,\
            AE.ENDERECO_ENTREGA as DELIVERYADDRESS,\
            to_char(AE.COD_AGENTE||'-'||A.COD_AGENTE) as PROJECTCODE,\
            AE.NOME as PROJECTNAME \
            FROM ODIN_MFARTEFATOS.ORDEM_PRODUCAO_REMESSA_PEDIDO OPRP\
            inner join ODIN_MFARTEFATOS.ORDEM_PRODUCAO_REMESSA OPR on OPRP.NRO_ORDEM_PRODUCAO_REMESSA=OPR.NRO_ORDEM_PRODUCAO_REMESSA\
            inner join ODIN_MFARTEFATOS.PEDIDO P on OPRP.NRO_PEDIDO=P.NRO_PEDIDO \
            inner join ODIN_MFARTEFATOS.AGENTE A on A.COD_AGENTE=P.COD_AGENTE \
            inner join ODIN_MFARTEFATOS.AGENTE AE on AE.COD_AGENTE=P.COD_AGENTE_ENTREGA \
            inner join ODIN_MFARTEFATOS.ITEM I on I.COD_ITEM=OPR.COD_ITEM "

        if last_updated_date is not None:
            sql += "where TO_CHAR(OPR.data_lancamento,'YYYY-MM-DD HH24:MI:SS') > :last_updated_date "
        else:
            sql += "where TO_CHAR(OPR.data_lancamento,'YYYY-MM-DD') >= :current_date "

        sql += "order by OPR.data_lancamento desc"

        return sql

    @staticmethod
    def build_query_orders(last_order_code:int = None):

        sql = "select "
        sql += "P.NumPedido as order_code, "
        sql += "convert(varchar,Min(PD.HoraInicio),20) as delivery_date, "
        sql += "'01' as plant_code, "
        sql += "Concat(CP.Codigo,'-',O.Codigo) as project_code, "
        sql += "O.Nombre as project_name, "
        sql += "CP.Codigo as customer_code, "
        sql += "CP.Nombre as customer_name, "
        sql += "Concat(CP.Codigo,'-',O.Codigo) as address_code, "
        sql += "O.Direccion as address_street, "
        sql += "O.Direccion as address_full, "
        sql += "O.Provincia as city_code, "
        sql += "O.Provincia as city_name, "
        sql += "EH.Codigo as usage_code, "
        sql += "EH.Texto as usage_name, "
        sql += "(Case when P.Activo ='S' and P.EnProduccion = 'S' and P.PendienteAviso = 'N' then 'A' else 'B' End) as status_code, "
        sql += "(Case when P.Activo ='S' and P.EnProduccion = 'S' and P.PendienteAviso = 'N' then 'Aprobado' else 'Bloqueado' End) as status_name, "
        sql += "P.Observaciones as customer_notes, "
        sql += "'ST' as tax_code, "
        sql += "'Estandar' as tax_name, "
        sql += "0 as tax_value, "
        sql += "FP.NomenclaturaInterna as product_code, "
        sql += "1 as product_type, "
        sql += "FP.Denominacion as product_name, "
        sql += "P.VolInicial+P.VolModificado as quantity "
        sql += "from Pedidos P "
        sql += "inner join PedidosDetalles PD on PD.IdPedido=P.Id "
        sql += "inner join ClientesPedidos CP on CP.IdPedido=P.Id "
        sql += "inner join Clientes C on C.Codigo=CP.Codigo "
        sql += "inner join ObrasPedidos OP on OP.IdPedido=P.Id "
        sql += "inner join Obras O on O.Codigo=OP.Codigo and C.Id=O.IdCliente "
        sql += "inner join FormulasPedidos FP on FP.IdPedido=P.Id "
        sql += "inner join ElementosHormigonado EH on FP.IdElementoHormigonado=EH.Id "
        sql += "where P.Fecha >= GETDATE() "
        sql += "group by "
        sql += "P.NumPedido,CP.Codigo,O.Codigo,O.Nombre,CP.Nombre,O.Direccion,O.Provincia,EH.Codigo,EH.Texto,P.Activo,P.EnProduccion,P.PendienteAviso, "
        sql += "P.Observaciones,FP.NomenclaturaInterna,FP.Denominacion,P.VolInicial,P.VolModificado "
        sql += "Order by P.NumPedido,FP.NomenclaturaInterna "

        # if last_order_code is not None:
        #     sql += "AND P.NRO_PEDIDO > :last_order_code "
        # else:
        #     sql += "AND to_char(P.DATA_ENTREGA,'YYYY-MM-DD') >= :current_date "
        #
        # sql += "ORDER BY P.NRO_PEDIDO DESC"

        return sql

    @staticmethod
    def dict_row_factory(cursor):
        columnNames = [d[0] for d in cursor.description]

        def createRow(*args):
            return dict(zip(columnNames, args))

        return createRow

    @staticmethod
    def dict_row_factory_lc(cursor):
        columnNames = [d[0].lower() for d in cursor.description]

        def createRow(*args):
            return dict(zip(columnNames, args))

        return createRow
