
/* declaracion de variables*/
val ID_DATE : String  = s"20230531"
val ID_DATE_ANT : String = s"20230430"
val PAIS : String  = s"GT"
var ID_PAIS = if(PAIS == "GT"){"502"} else {
        if(PAIS == "SV"){"503"} else {
        if(PAIS == "HN"){"504"} else {
        if(PAIS == "NI"){"505"} else {
        if(PAIS == "CR"){"506"} else {
        if(PAIS == "PA"){"507"} }}}}}
val ID_ANIO = ID_DATE.slice(0, 4)
val ID_MES = ID_DATE.slice(4, 6)
val ID_DIA = ID_DATE.slice(6, 8)
val ID_MESM1 = ID_DATE_ANT.slice(4, 6)
val ID_ANIOM1 = ID_DATE_ANT.slice(0, 4)
spark.sql("set ID_DATE = "+ID_DATE)
spark.sql("set ID_ANIO = "+ID_ANIO)
spark.sql("set ID_MES = "+ID_MES)
spark.sql("set ID_DIA = "+ID_DIA)
spark.sql("set ID_PAIS = "+ID_PAIS)
spark.sql("set PAIS = "+PAIS)
spark.sql("set ID_MCC = "+ID_MCC)
spark.sql("set PROYECTO = "+PROYECTO)
spark.sql("set ID_MESM1 = "+ID_MESM1)
spark.sql("set ID_ANIOM1 = "+ID_ANIOM1)
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
spark.sql("CREATE TEMPORARY FUNCTION distancia AS 'FuncionesHive.KM_BETWEEN'");

/*tabla de sitios*/

var tblsitios = sql("""
select 'OAKLAND MALL' Lugar, '14.59853' latitud, '-90.50769' longitud
union all
select 'ESKALA R' Lugar, '14.636888' latitud, '-90.582347' longitud
union all
select 'EL FRUTAL' Lugar, '14.521307' latitud, '-90.564254' longitud
""")
tblsitios.createOrReplaceTempView("bi.sitios")


/*calculo de celdas de interes por distancia*/

var tblceldas = sql("""
select distinct * from(
select distinct
       a.Lugar,
       cast (a.latitud as double) lat_lugar,
       cast (a.longitud as double) lon_lugar,
       cast(b.latitud as double) latitud,
       cast(b.longitud as double) longitud,
       round(distancia(cast(trim(b.latitud) as double), cast(trim(b.longitud) as double), cast(a.latitud as double), cast(a.longitud as double))) distancia_metros,
       b.cgi,
       b.id,
       b.name nombre_celda,
       b.departamento,
       b.municipio,
       b.zona,
       b.tecnologia
from  bi.sitios a
cross join (
    select * from
        bi.cell_data
    where
        pais = '${hiveconf:ID_PAIS}') b on b.latitud IS NOT NULL
WHERE b.latitud rlike '[^0-9]'
    and b.latitud is not null
    and b.longitud rlike '[^0-9]'
    and b.longitud is not null
    and substr(b.longitud,1,1)='-'
    )x
    where distancia_metros <= 5000
    order by distancia_metros desc
""")
tblceldas.createOrReplaceTempView("tblceldas")

/*base de telefonos segun su celda hogar*/

sql("drop table if exists bi.tb_base_h")
sql("""
create table bi.tb_base_h stored as orc
    select
        b.lugar nombre_sitio,
        a.phone,
        c.genero,
        c.rango_edad,
        d.nse,
        b.lat_lugar,
        b.lon_lugar,
        b.latitud,
        b.longitud,
        e.tac,
        f.gsma_device_type,
        g.status,
        b.distancia_metros
    from bi.mov_logistica a
    inner join tblceldas b
    on a.h_cell = b.cgi
    left join (
        select * from
        bi.rango_edad_genero
        where
            pais = '${hiveconf:ID_PAIS}' and
            mes = '${hiveconf:ID_ANIOM1}${hiveconf:ID_MESM1}') c
    on a.phone = c.telefono
    left join (
        select * from
        bi.nse
        where
            pais = '${hiveconf:ID_PAIS}' and
            mes = '${hiveconf:ID_ANIOM1}${hiveconf:ID_MESM1}') d
    on a.phone = d.telefono
    left join (
        select
            telefono,
            imei,
            tac
        from bi.parque_imei
        where
            pais = '${hiveconf:ID_PAIS}' and
            mes = '${hiveconf:ID_ANIO}${hiveconf:ID_MES}') e
    on a.phone = e.telefono
    left join  bi.cat_gsma f
    on e.tac = f.tac
    left join(
        select distinct telefono, status from
        bi.seg_parq_movil
        where
            pais = '${hiveconf:ID_PAIS}' and
            mes = '${hiveconf:ID_ANIO}${hiveconf:ID_MES}') g
    on a.phone = g.telefono
    where
        a.mes = '${hiveconf:ID_ANIO}${hiveconf:ID_MES}'
    order by
        a.phone asc""")

/*base de telefonos segun su celda de trabajo*/

sql("drop table if exists bi.tb_base_w")
sql("""
create table bi.tb_base_w stored as orc
    select
        b.lugar nombre_sitio,
        a.phone,
        c.genero,
        c.rango_edad,
        d.nse,
        b.lat_lugar,
        b.lon_lugar,
        b.latitud,
        b.longitud,
        e.tac,
        f.gsma_device_type,
        g.status,
        b.distancia_metros
    from bi.mov_logistica a
    inner join tblceldas b
    on a.w_cell = b.cgi
    left join (
        select * from
        bi.rango_edad_genero
        where
            pais = '${hiveconf:ID_PAIS}' and
            mes = '${hiveconf:ID_ANIOM1}${hiveconf:ID_MESM1}') c
    on a.phone = c.telefono
    left join (
        select * from
        bi.nse
        where
            pais = '${hiveconf:ID_PAIS}' and
            mes = '${hiveconf:ID_ANIOM1}${hiveconf:ID_MESM1}') d
    on a.phone = d.telefono
    left join (
        select
            telefono,
            imei,
            tac
        from bi.parque_imei
        where
            pais = '${hiveconf:ID_PAIS}' and
            mes = '${hiveconf:ID_ANIO}${hiveconf:ID_MES}') e
    on a.phone = e.telefono
    left join bi.cat_gsma f
    on e.tac = f.tac
    left join(
        select distinct telefono, status from
        bi.seg_parq_movil
        where
            pais = '${hiveconf:ID_PAIS}' and
            mes = '${hiveconf:ID_ANIO}${hiveconf:ID_MES}') g
    on a.phone = g.telefono
    where
        a.mes = '${hiveconf:ID_ANIO}${hiveconf:ID_MES}'
    order by
        a.phone asc
""")

/*tabla concatenada*/

sql("drop table if exists bi.tb_base_h_w")
sql("""
create table bi.tb_base_h_w stored as orc
    select
        a.nombre_sitio,
        a.phone,
        a.genero,
        a.rango_edad,
        a.nse,
        a.lat_lugar,
        a.lon_lugar,
        a.latitud,
        a.longitud,
        a.gsma_device_type,
        a.distancia_metros,
        a.status,
        "HOGAR" segmento_ubicacion
    from (
        select 
            nombre_sitio,
            phone,
            genero,
            rango_edad,
            nse,
            lat_lugar,
            lon_lugar,
            latitud,
            longitud,
            gsma_device_type,
            status,
            distancia_metros
        from bi.tb_base_h where gsma_device_type in ('Mobile Phone/Feature phone', 'Smartphone', 'Handheld')
        and status in (2,3,4,'A')) a
    left join (select * from bi.tb_base_w) b on a.phone = b.phone where b.phone is null
    union all
    select
        a.nombre_sitio,
        a.phone,
        a.genero,
        a.rango_edad,
        a.nse,
        a.lat_lugar,
        a.lon_lugar,
        a.latitud,
        a.longitud,
        a.gsma_device_type,
        a.distancia_metros,
        a.status,
        "TRABAJO" segmento_ubicacion
    from (
        select 
            nombre_sitio,
            phone,
            genero,
            rango_edad,
            nse,
            lat_lugar,
            lon_lugar,
            latitud,
            longitud,
            gsma_device_type,
            status,
            distancia_metros
        from bi.tb_base_w
        where
            gsma_device_type in ('Mobile Phone/Feature phone', 'Smartphone', 'Handheld')
            and status in (2,3,4,'A')) a
    left join (select * from bi.tb_base_h ) b on a.phone = b.phone where b.phone is null
    union all
    select
        a.nombre_sitio,
        a.phone,
        a.genero,
        a.rango_edad,
        a.nse,
        a.lat_lugar,
        a.lon_lugar,
        a.latitud,
        a.longitud,
        a.gsma_device_type,
        a.distancia_metros,
        a.status,
        "HOGAR/TRABAJO" segmento_ubicacion
    from (
        select 
            nombre_sitio,
            phone,
            genero,
            rango_edad,
            nse,
            lat_lugar,
            lon_lugar,
            latitud,
            longitud,
            gsma_device_type,
            status,
            distancia_metros
        from bi.tb_base_h
        where
            gsma_device_type in ('Mobile Phone/Feature phone', 'Smartphone', 'Handheld') 
            and status in (2,3,4,'A')) a
    inner join (
        select 
            nombre_sitio,
            phone,
            genero,
            rango_edad,
            nse,
            lat_lugar,
            lon_lugar,
            latitud,
            longitud,
            gsma_device_type,
            status,
            distancia_metros
        from bi.tb_base_w
        where
            gsma_device_type in (
                'Mobile Phone/Feature phone','Smartphone', 'Handheld')
                and status in (2,3,4,'A')) b on a.phone = b.phone
    """)

/*filtro para base final de telefonos*/

sql("drop table if exists bi.base_num_final")
sql("""
create table bi.base_num_final stored as orc
select nombre_sitio, lat_lugar,lon_lugar, latitud, longitud, phone telefono, genero, rango_edad, nse, segmento_ubicacion, rango_distancia  from
        (   
            select
            nombre_sitio,
            phone,
            genero,
            rango_edad,
            nse,
            lat_lugar,
            lon_lugar,
            latitud,
            longitud,
            gsma_device_type, 
            segmento_ubicacion,
            case when distancia_metros between 0 and 1000 then "1km"
            when distancia_metros between 1000 and 2000 then "2km"
            when distancia_metros between 2000 and 3000 then "3km"
            when distancia_metros between 3000 and 4000 then "4km"
            when distancia_metros between 4000 and 5000 then "5km"
            else "" end rango_distancia,
            row_number () over (partition by phone order by distancia_metros asc) ro_num
        from (
                select * from bi.tb_base_h_w
            )) a
where a.ro_num = 1
""")