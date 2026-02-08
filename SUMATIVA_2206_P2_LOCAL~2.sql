/* ===========================================================
   SUMATIVA 2 - PRY2206
   Bloque PL/SQL Anónimo: Aportes SBIF (Avance y Súper Avance)

   Idea general:
   - Tomo el año desde una variable bind (paramétrico)
   - Busco todas las transacciones del año para tipos 102 y 103
   - Cargo detalle en DETALLE_APORTE_SBIF
   - Cargo resumen en RESUMEN_APORTE_SBIF
   - TRUNCATE en ejecución para poder repetir el proceso
   - COMMIT solo si se procesó todo lo esperado
   =========================================================== */

-- (Opcional) Confirmar usuario conectado
SELECT USER FROM dual;

SET SERVEROUTPUT ON;

--------------------------------------------------------------
-- 1) VARIABLE BIND (año paramétrico)
--    Esto permite ejecutar el proceso para cualquier año
--------------------------------------------------------------
VAR v_anno NUMBER;

BEGIN
  :v_anno := 2026;  -- Cambia aquí si quieres probar otro año
END;
/
PRINT v_anno;

--------------------------------------------------------------
-- 2) BLOQUE PL/SQL PRINCIPAL
--------------------------------------------------------------
DECLARE
  -- Traigo el año desde el bind
  v_anno NUMBER := :v_anno;

  -- VARRAY: guardo los códigos de transacción que me interesan
  -- 102 = Avance en efectivo
  -- 103 = Súper avance en efectivo
  TYPE t_varray_tipos IS VARRAY(2) OF NUMBER;
  v_tipos t_varray_tipos := t_varray_tipos(102, 103);

  -- Registro PL/SQL para guardar una fila del cursor detalle
  TYPE r_det IS RECORD(
    numrun            CLIENTE.numrun%TYPE,
    dvrun             CLIENTE.dvrun%TYPE,
    nro_tarjeta       TRANSACCION_TARJETA_CLIENTE.nro_tarjeta%TYPE,
    nro_transaccion   TRANSACCION_TARJETA_CLIENTE.nro_transaccion%TYPE,
    fecha_transaccion TRANSACCION_TARJETA_CLIENTE.fecha_transaccion%TYPE,
    tipo_transaccion  TIPO_TRANSACCION_TARJETA.nombre_tptran_tarjeta%TYPE,
    monto_transaccion TRANSACCION_TARJETA_CLIENTE.monto_transaccion%TYPE,
    monto_total       TRANSACCION_TARJETA_CLIENTE.monto_total_transaccion%TYPE
  );
  v_row r_det;

  -- Variables para controlar que el proceso termine bien
  v_total_esperado   NUMBER := 0; -- cuántas transacciones debería procesar
  v_total_procesado  NUMBER := 0; -- cuántas procesé realmente

  -- Variables para calcular aporte
  v_pct_aporte NUMBER := 0;
  v_aporte     NUMBER := 0;

  ------------------------------------------------------------
  -- Excepciones
  ------------------------------------------------------------
  -- Excepción definida por el usuario: si no hay transacciones
  e_sin_transacciones  EXCEPTION;

  -- Excepción definida por el usuario: si no se procesó todo
  e_proceso_incompleto EXCEPTION;

  -- Excepción NO predefinida: error de FK (ORA-02292)
  e_fk_child_exists EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_fk_child_exists, -2292);

  ------------------------------------------------------------
  -- CURSOR 1 (explícito): detalle
  -- Ordenado por fecha y numrun como pide el enunciado
  ------------------------------------------------------------
  CURSOR c_detalle IS
    SELECT
      c.numrun,
      c.dvrun,
      t.nro_tarjeta,
      t.nro_transaccion,
      t.fecha_transaccion,
      tp.nombre_tptran_tarjeta AS tipo_transaccion,
      t.monto_transaccion,
      t.monto_total_transaccion
    FROM TRANSACCION_TARJETA_CLIENTE t
    JOIN TARJETA_CLIENTE tc
      ON tc.nro_tarjeta = t.nro_tarjeta
    JOIN CLIENTE c
      ON c.numrun = tc.numrun
    JOIN TIPO_TRANSACCION_TARJETA tp
      ON tp.cod_tptran_tarjeta = t.cod_tptran_tarjeta
    WHERE EXTRACT(YEAR FROM t.fecha_transaccion) = v_anno
      AND t.cod_tptran_tarjeta IN (v_tipos(1), v_tipos(2))
    ORDER BY t.fecha_transaccion, c.numrun;

  ------------------------------------------------------------
  -- CURSOR 2 (explícito con parámetro): resumen por mes y tipo
  ------------------------------------------------------------
  CURSOR c_resumen(p_mes NUMBER, p_cod_tipo NUMBER) IS
    SELECT
      TO_CHAR(t.fecha_transaccion,'MMYYYY') AS mes_anno,
      tp.nombre_tptran_tarjeta              AS tipo_transaccion,
      SUM(t.monto_total_transaccion)        AS monto_total_transacciones
    FROM TRANSACCION_TARJETA_CLIENTE t
    JOIN TIPO_TRANSACCION_TARJETA tp
      ON tp.cod_tptran_tarjeta = t.cod_tptran_tarjeta
    WHERE EXTRACT(YEAR  FROM t.fecha_transaccion) = v_anno
      AND EXTRACT(MONTH FROM t.fecha_transaccion) = p_mes
      AND t.cod_tptran_tarjeta = p_cod_tipo
    GROUP BY TO_CHAR(t.fecha_transaccion,'MMYYYY'), tp.nombre_tptran_tarjeta;

  ------------------------------------------------------------
  -- FUNCIÓN: calcula el % de aporte según TRAMO_APORTE_SBIF
  -- Ojo: el cálculo se hace en PL/SQL (como pide el enunciado)
  ------------------------------------------------------------
  FUNCTION fn_pct_aporte(p_monto_total NUMBER) RETURN NUMBER IS
    v_pct NUMBER;
  BEGIN
    SELECT porc_aporte_sbif
      INTO v_pct
      FROM TRAMO_APORTE_SBIF
     WHERE p_monto_total BETWEEN tramo_inf_av_sav AND tramo_sup_av_sav;

    RETURN v_pct;

  EXCEPTION
    -- Excepción predefinida: si no existe tramo, dejo 0%
    WHEN NO_DATA_FOUND THEN
      RETURN 0;
  END;

BEGIN
  DBMS_OUTPUT.PUT_LINE('== INICIO PROCESO ==');
  DBMS_OUTPUT.PUT_LINE('AÑO = ' || v_anno);

  ----------------------------------------------------------
  -- TRUNCATE en tiempo de ejecución (para ejecutar varias veces)
  ----------------------------------------------------------
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DETALLE_APORTE_SBIF';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RESUMEN_APORTE_SBIF';

  ----------------------------------------------------------
  -- Total esperado: sirve para comparar con el contador
  ----------------------------------------------------------
  SELECT COUNT(*)
    INTO v_total_esperado
    FROM TRANSACCION_TARJETA_CLIENTE t
   WHERE EXTRACT(YEAR FROM t.fecha_transaccion) = v_anno
     AND t.cod_tptran_tarjeta IN (v_tipos(1), v_tipos(2));

  DBMS_OUTPUT.PUT_LINE('TOTAL ESPERADO = ' || v_total_esperado);

  IF v_total_esperado = 0 THEN
    RAISE e_sin_transacciones;
  END IF;

  ----------------------------------------------------------
  -- Cargar DETALLE_APORTE_SBIF (fila por fila con cursor)
  ----------------------------------------------------------
  OPEN c_detalle;
  LOOP
    FETCH c_detalle INTO v_row;
    EXIT WHEN c_detalle%NOTFOUND;

    v_total_procesado := v_total_procesado + 1;

    -- Calculo aporte (redondeando como pide el enunciado)
    v_pct_aporte := fn_pct_aporte(ROUND(v_row.monto_total));
    v_aporte     := ROUND(ROUND(v_row.monto_total) * (v_pct_aporte / 100));

    INSERT INTO DETALLE_APORTE_SBIF(
      numrun, dvrun, nro_tarjeta, nro_transaccion,
      fecha_transaccion, tipo_transaccion,
      monto_transaccion, aporte_sbif
    ) VALUES (
      v_row.numrun, v_row.dvrun, v_row.nro_tarjeta, v_row.nro_transaccion,
      v_row.fecha_transaccion, v_row.tipo_transaccion,
      ROUND(v_row.monto_transaccion), v_aporte
    );
  END LOOP;
  CLOSE c_detalle;

  ----------------------------------------------------------
  -- Cargar RESUMEN_APORTE_SBIF (mes y tipo)
  ----------------------------------------------------------
  FOR i IN 1..12 LOOP
    FOR j IN 1..v_tipos.COUNT LOOP
      FOR r IN c_resumen(i, v_tipos(j)) LOOP
        DECLARE
          v_aporte_total NUMBER := 0;
        BEGIN
          -- Sumo aportes desde detalle para el mes y tipo
          SELECT NVL(SUM(d.aporte_sbif),0)
            INTO v_aporte_total
            FROM DETALLE_APORTE_SBIF d
           WHERE EXTRACT(YEAR  FROM d.fecha_transaccion) = v_anno
             AND EXTRACT(MONTH FROM d.fecha_transaccion) = i
             AND d.tipo_transaccion = r.tipo_transaccion;

          INSERT INTO RESUMEN_APORTE_SBIF(
            mes_anno, tipo_transaccion, monto_total_transacciones, aporte_total_abif
          ) VALUES (
            r.mes_anno, r.tipo_transaccion,
            ROUND(r.monto_total_transacciones), ROUND(v_aporte_total)
          );
        END;
      END LOOP;
    END LOOP;
  END LOOP;

  ----------------------------------------------------------
  -- COMMIT solo si procesé TODO lo esperado (regla del enunciado)
  ----------------------------------------------------------
  IF v_total_procesado = v_total_esperado THEN
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('OK COMMIT -> '||v_total_procesado||' / '||v_total_esperado);
  ELSE
    ROLLBACK;
    RAISE e_proceso_incompleto;
  END IF;

  DBMS_OUTPUT.PUT_LINE('== FIN PROCESO ==');

EXCEPTION
  WHEN e_sin_transacciones THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('No hay transacciones para el año '||v_anno||' (tipos 102/103).');

  WHEN e_proceso_incompleto THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('Proceso incompleto: '||v_total_procesado||' de '||v_total_esperado||'. Se hace ROLLBACK.');

  WHEN DUP_VAL_ON_INDEX THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('Error: PK repetida al insertar en DETALLE_APORTE_SBIF.');

  WHEN e_fk_child_exists THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('Error ORA-02292: hay registros relacionados (FK), no se puede borrar/insertar como corresponde.');

  WHEN OTHERS THEN
    ROLLBACK;
    DBMS_OUTPUT.PUT_LINE('ERROR: '||SQLERRM);
    DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
END;
/

--------------------------------------------------------------
-- 3) VALIDACIONES (para evidencias / capturas)
--------------------------------------------------------------

SELECT COUNT(*) AS filas_detalle FROM DETALLE_APORTE_SBIF;
SELECT COUNT(*) AS filas_resumen FROM RESUMEN_APORTE_SBIF;

SELECT *
FROM DETALLE_APORTE_SBIF
ORDER BY fecha_transaccion, numrun;

SELECT *
FROM RESUMEN_APORTE_SBIF
ORDER BY mes_anno, tipo_transaccion;

SELECT cod_tptran_tarjeta, COUNT(*) cantidad
FROM TRANSACCION_TARJETA_CLIENTE
WHERE EXTRACT(YEAR FROM fecha_transaccion) = :v_anno
GROUP BY cod_tptran_tarjeta
ORDER BY cod_tptran_tarjeta;
