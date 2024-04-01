session:date-format = "dmy".

DEFINE TEMP-TABLE TmpDeltas NO-UNDO 
    FIELD TmpSerie   AS CHARACTER FORMAT "x(5)"
    FIELD TmpFolEdi  AS decimal   
    FIELD TmpNoPoli  AS CHARACTER FORMAT "x(14)"
    FIELD TmpNoEndo  AS CHARACTER FORMAT "x(10)"
    FIELD TmpFolio   AS CHARACTER FORMAT "x(10)"
    FIELD TmpUnico   AS CHARACTER FORMAT "x(10)"
    FIELD FolPoliza  AS CHARACTER FORMAT "x(12)"   
    FIELD FolEndoso  AS CHARACTER FORMAT "x(12)" 
    FIELD FolTipFac  AS CHARACTER FORMAT "x(3)"
    FIELD FolStaFol  AS CHARACTER FORMAT "x(20)"
    FIELD FolStaPag  AS LOGICAL   FORMAT "Si/No"
    FIELD FolStaEnv  AS INTEGER   FORMAT "9"
    FIELD FolFecCre  AS DATE
    FIELD FolFecEnv  AS DATE
    FIELD FolIntern  AS INTEGER   FORMAT ">>>>>>9"
    FIELD FolNumFac  AS INTEGER   FORMAT ">>>>>>>9"
    FIELD FolNumErr  AS INTEGER   FORMAT ">9"        
    FIELD TraSerie   AS CHARACTER FORMAT "x(5)"
    FIELD TraFolio   AS decimal   
    FIELD TraStaFol  AS CHARACTER FORMAT "x(20)"
    FIELD TraFecCre  AS DATE
    FIELD TraFecEnv  AS DATE
    FIELD TraStaPag  AS LOGICAL   FORMAT "Si/No" 
    FIELD TraPoliza  AS CHARACTER FORMAT "x(12)"   
    FIELD TraEndoso  AS CHARACTER FORMAT "x(12)" 
    FIELD TraNumFac  AS INTEGER   FORMAT ">>>>>>>9"
    FIELD TraIntern  AS INTEGER   FORMAT ">>>>>>9"
    FIELD PNeta      AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD Imp-Reduc  AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD Recargo    AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD Derechos   AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD Impuesto   AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD PTotal     AS DECIMAL   FORMAT "->>>,>>>,>>9.99"
    FIELD Comision   AS DECIMAL   FORMAT "->>>,>>>,>>9.99" 
    FIELD TraTipo    AS CHARACTER FORMAT "x(5)"
    FIELD OutVig     AS CHARACTER FORMAT "x(2)"
    FIELD Brinco     AS CHARACTER FORMAT "x(2)"
    FIELD TraFecIni  AS DATE
    FIELD TraFecFin  AS DATE   .
 
DEFINE BUFFER BufFoliosSat for FoliosSat.    
DEFINE temp-table BufTmpDeltas NO-UNDO like TmpDeltas.

DEFINE TEMP-TABLE TmpClaves    NO-UNDO like cobran.claves.
DEFINE TEMP-TABLE TmpFoliosSAT NO-UNDO like cobran.FoliosSAT.

DEFINE VARIABLE wconta     AS INTEGER                       no-undo.
DEFINE VARIABLE wtotal     AS INTEGER                       no-undo.
DEFINE VARIABLE vchrStCFD1 AS CHARACTER FORMAT "X(16)"      no-undo.
DEFINE VARIABLE wFolioSat  LIKE TmpDeltas.TraFolio          no-undo.
DEFINE VARIABLE wFolioSat2 LIKE TmpDeltas.TraFolio          no-undo.
DEFINE VARIABLE wdelta     AS CHARACTER FORMAT "X(5)"       no-undo.
DEFINE VARIABLE wSinVi     AS CHARACTER FORMAT "X(2)"       no-undo.
DEFINE VARIABLE wBrinco    AS CHARACTER FORMAT "X(2)"       no-undo.
DEFINE VARIABLE wvan       AS INTEGER                       no-undo.
DEFINE VARIABLE wllave     AS CHARACTER FORMAT "X(32)"      no-undo. 


STATUS DEFAULT "Importando.....".    
INPUT FROM "v:\DELTAS_2022.csv".
/*do wconta = 1 to 7000 :*/ 
REPEAT: 
  CREATE TmpDeltas.
  IMPORT DELIMITER ";" TmpDeltas no-error.
  wtotal = wtotal + 1.
END.
/*wconta = 0. */ 
INPUT CLOSE. 

for each cobran.claves where cobran.claves.ramo     = "10" 
                         AND cobran.claves.cvecampo = "EstFolioSat" no-lock :
    create TmpClaves.
    buffer-copy cobran.claves to TmpClaves.
end. 

OUTPUT TO v:\ResulDeltas2022.txt.
EXPORT DELIMITER "|" 
        "Delta Serie" "Delta Folio Edi" "Delta Poliza" "Delta Endoso" "Delta Folio" "Delta Unico"
        "CFD Poliza" "CFD Endoso" "CFD TipFact" "CFD Stat Folio" "CFD Stat Pagado" "CFD Stat Enviado" "CFD Fec Crea" 
            "CFD Fec Envio" "CFD Folio Interno" "CFD No. Factura" "CFD No.Error"
        "Traza Serie" "Traza Folio" "Traza Stat Folio" "Traza Fec Crea" "Traza Fec Env" "Traza Stat Pagado"
            "Traza Poliza" "Traza Endoso" "Traza No Factura" "Traza Folio Interno"
        "Fact PNeta" "Fact Imp-Reducción" "Fact Recargo" "Fact Derechos" "Fact Impuesto" "Fact PTotal" "Fact Comision"
        "Delta" "Sin Vigencia" "Brinco A¤o" "Fec Ini Vig" "Fec Fin Vig".
            
FOR EACH TmpDeltas :
    wconta = wconta + 1.
    STATUS DEFAULT "Procesando Reg " + string(wconta,">,>>>,>>>") + " de " + string(wtotal,">,>>>,>>>"). 
    if TmpDeltas.TmpFolEdi = 0 THEN next.

    FIND FIRST foliosSat WHERE foliosSat.SerieSAT = TmpDeltas.TmpSerie 
                           AND foliosSat.FolioSAT = TmpDeltas.TmpFolEdi NO-LOCK NO-ERROR.
    
    IF AVAILABLE foliosSat THEN DO :
       FIND FIRST poliza where poliza.poliza = FoliosSAT.Poliza no-lock no-error.
       FIND FIRST Tmpclaves WHERE Tmpclaves.statcve = FoliosSAT.EstFolioSat NO-ERROR.
       vchrStCFD1 = string(FoliosSat.EstFolioSat,">9 ") + 
                    IF AVAILABLE Tmpclaves THEN ENTRY(2,TmpClaves.Descrip,"|") ELSE "".
              
       ASSIGN TmpDeltas.FolPoliza    = FoliosSAT.Poliza  
              TmpDeltas.FolEndoso    = FoliosSAT.Endoso
              TmpDeltas.FolTipFac    = FoliosSAT.tipfact
              TmpDeltas.FolStaFol    = vchrStCFD1 
              TmpDeltas.FolStaPag    = FoliosSAT.EstFolioPagado
              TmpDeltas.FolStaEnv    = FoliosSAT.EstFolioEnvio
              TmpDeltas.FolFecCre    = FoliosSAT.Fec_Creacion
              TmpDeltas.FolFecEnv    = FoliosSAT.Fec_Envio 
              TmpDeltas.FolIntern    = FoliosSAT.FolioInt
              TmpDeltas.FolNumFac    = FoliosSAT.numfact
              TmpDeltas.FolNumErr    = FoliosSAT.NumError.
       if available poliza then assign TmpDeltas.TraFecIni = poliza.fec-ini
                                       TmpDeltas.TraFecFin = poliza.fec-ter.
       if wllave <> (FoliosSAT.Poliza + FoliosSAT.Endoso + string(FoliosSAT.numfact)) then do :
          wllave = (FoliosSAT.Poliza + FoliosSAT.Endoso + string(FoliosSAT.numfact)).  
          empty temp-table TmpFoliosSAT.
          for each BufFoliosSat WHERE BufFoliosSat.Poliza    = FoliosSAT.Poliza  AND
                                      BufFoliosSAT.Endoso    = FoliosSAT.Endoso  AND
                                      BufFoliosSat.NumFact   = FoliosSAT.NumFact NO-LOCK :
                create TmpFoliosSAT.
                buffer-copy BufFoliosSat to TmpFoliosSAT.
          end.             
       end.       
       if TmpDeltas.TmpSerie = "GMD" 
       then FIND FIRST TmpFoliosSAT WHERE TmpFoliosSAT.SerieSAT = TmpDeltas.TmpSerie 
                                      AND TmpFoliosSAT.FolioSAT = TmpDeltas.TmpFolEdi NO-LOCK NO-ERROR.  
       else FIND FIRST TmpFoliosSAT WHERE TmpFoliosSAT.Poliza    = FoliosSAT.Poliza  AND
                                          TmpFoliosSAT.Endoso    = FoliosSAT.Endoso  AND
                                          TmpFoliosSAT.NumFact   = FoliosSAT.NumFact NO-LOCK NO-ERROR.
       IF AVAILABLE TmpFoliosSAT then DO :
          FIND FIRST Tmpclaves WHERE Tmpclaves.statcve = TmpFoliosSAT.EstFolioSat NO-ERROR.
          vchrStCFD1 = string(TmpFoliosSAT.EstFolioSat,">9 ") +
                       IF AVAILABLE Tmpclaves THEN ENTRY(2,TmpClaves.Descrip,"|") ELSE "".
     
          ASSIGN TmpDeltas.TraSerie  = TmpFoliosSAT.SerieSat
                 TmpDeltas.TraFolio  = TmpFoliosSAT.FolioSAT 
                 TmpDeltas.TraStaFol = vchrStCFD1
                 TmpDeltas.TraFecCre = TmpFoliosSAT.Fec_Creacion
                 TmpDeltas.TraFecEnv = TmpFoliosSAT.Fec_Envio
                 TmpDeltas.TraStaPag = TmpFoliosSAT.EstFolioPagado
                 TmpDeltas.TraPoliza = TmpFoliosSAT.Poliza   
                 TmpDeltas.TraEndoso = TmpFoliosSAT.Endoso
                 TmpDeltas.TraIntern = TmpFoliosSAT.FolioInt
                 TmpDeltas.TraNumFac = TmpFoliosSAT.numfact.
                 wFolioSat2 = TmpFoliosSAT.FolioSAT.                                 
          Run Facturas (1).
       END.
       
    END.          
    EXPORT DELIMITER "|"  TmpDeltas.
    
    IF not AVAILABLE foliosSat THEN next.
    
    wFolioSat = TmpDeltas.TraFolio.
    empty temp-table BufTmpDeltas.
    CREATE BufTmpDeltas.
    BUFFER-COPY TmpDeltas EXCEPT TmpDeltas.FolPoliza TmpDeltas.FolEndoso TmpDeltas.FolTipFac
                                 TmpDeltas.FolStaFol TmpDeltas.FolStaPag TmpDeltas.FolStaEnv
                                 TmpDeltas.FolFecCre TmpDeltas.FolFecEnv TmpDeltas.FolIntern
                                 TmpDeltas.FolNumFac TmpDeltas.FolNumErr TmpDeltas.TraTipo 
                          TO BufTmpDeltas.
    CASE TmpDeltas.TmpSerie:
        WHEN "GMC" THEN for each TmpFoliosSAT WHERE TmpFoliosSAT.Poliza    = FoliosSAT.Poliza   AND
                                                    TmpFoliosSAT.Endoso    = FoliosSAT.Endoso   AND
                                                    TmpFoliosSAT.NumFact   = FoliosSAT.NumFact  AND
                                                    TmpFoliosSAT.SerieSat <> "GMD"              AND
                                                    TmpFoliosSAT.FolioSat <> wFolioSat NO-LOCK :
                            Run GeneraDetalle.
                        end.
 
        WHEN "GMD" THEN for each TmpFoliosSAT WHERE TmpFoliosSAT.Poliza    = FoliosSAT.Poliza   AND
                                                    TmpFoliosSAT.Endoso    = FoliosSAT.Endoso   AND
                                                    TmpFoliosSAT.NumFact   = FoliosSAT.NumFact  AND
                                                    TmpFoliosSAT.SerieSat <> "GMD" NO-LOCK :
                            Run GeneraDetalle.
                        end.      
        otherwise FOR EACH TmpFoliosSAT WHERE TmpFoliosSAT.Poliza    = FoliosSAT.Poliza   AND
                                              TmpFoliosSAT.Endoso    = FoliosSAT.Endoso   AND
                                              TmpFoliosSAT.NumFact   = FoliosSAT.NumFact  AND
                                              TmpFoliosSAT.FolioSat <> wFolioSat NO-LOCK :
                            Run GeneraDetalle.
                  END.
    END CASE.
      
END.
OUTPUT CLOSE.

MESSAGE wconta skip VIEW-AS ALERT-BOX.


PROCEDURE Facturas:
    define input parameter piproceso AS INTEGER.
    
    assign wdelta  = (if TmpFoliosSAT.FolioSAT = TmpDeltas.TmpFolEdi then "Delta" else "")     
           wSinVi  = ""
           wBrinco = "".

    if available poliza then do :
        if (TmpFoliosSAT.Fec_Creacion > poliza.fec-ter or
            TmpFoliosSAT.Fec_Envio    > poliza.fec-ter)
        then wSinVi = "Si".
        if year(TmpFoliosSAT.Fec_Creacion) > year(poliza.fec-ter)  or
           year(TmpFoliosSAT.Fec_Envio)    > year(poliza.fec-ter)  
        then wBrinco = "Si".
    end.        
   
    if piproceso = 1 then assign TmpDeltas.TraTipo    = wdelta
                                 TmpDeltas.OutVig     = wSinVi
                                 TmpDeltas.Brinco     = wBrinco.
                     else assign BufTmpDeltas.TraTipo = wdelta
                                 BufTmpDeltas.OutVig  = wSinVi
                                 BufTmpDeltas.Brinco  = wBrinco.
        
    FIND FIRST factura WHERE Factura.poliza  = TmpFoliosSAT.Poliza
                         AND Factura.endoso  = TmpFoliosSAT.Endoso
                         AND factura.numfact = TmpFoliosSAT.numfact NO-LOCK NO-ERROR.
    IF NOT AVAILABLE factura 
    then FIND FIRST factura WHERE factura.numfact = TmpFoliosSAT.numfact NO-LOCK NO-ERROR.               
    IF AVAILABLE factura THEN DO: 
       if piproceso = 1 
       then ASSIGN TmpDeltas.PNeta     = Factura.PNeta[1]
                   TmpDeltas.Imp-Reduc = Factura.Impte-Reduc
                   TmpDeltas.Recargo   = Factura.Recargo
                   TmpDeltas.Derechos  = Factura.Derecho
                   TmpDeltas.Impuesto  = Factura.Impuesto
                   TmpDeltas.PTotal    = Factura.PTotal
                   TmpDeltas.Comision  = Factura.Comis[1] + Factura.Comis[2] + Factura.Comis[3].
       else ASSIGN BufTmpDeltas.PNeta     = Factura.PNeta[1]
                   BufTmpDeltas.Imp-Reduc = Factura.Impte-Reduc
                   BufTmpDeltas.Recargo   = Factura.Recargo
                   BufTmpDeltas.Derechos  = Factura.Derecho
                   BufTmpDeltas.Impuesto  = Factura.Impuesto
                   BufTmpDeltas.PTotal    = Factura.PTotal
                   BufTmpDeltas.Comision  = Factura.Comis[1] + Factura.Comis[2] + Factura.Comis[3].          
    END.   
       
END PROCEDURE.
    
    
PROCEDURE GeneraDetalle:
    FIND FIRST Tmpclaves WHERE Tmpclaves.statcve = TmpFoliosSAT.EstFolioSat NO-ERROR.
    vchrStCFD1 = string(TmpFoliosSAT.EstFolioSat,">9 ") +
                 IF AVAILABLE Tmpclaves THEN ENTRY(2,TmpClaves.Descrip,"|") ELSE "".
    ASSIGN BufTmpDeltas.TraSerie  = TmpFoliosSAT.SerieSat
           BufTmpDeltas.TraFolio  = TmpFoliosSAT.FolioSAT 
           BufTmpDeltas.TraStaFol = vchrStCFD1
           BufTmpDeltas.TraFecCre = TmpFoliosSAT.Fec_Creacion
           BufTmpDeltas.TraFecEnv = TmpFoliosSAT.Fec_Envio
           BufTmpDeltas.TraStaPag = TmpFoliosSAT.EstFolioPagado
           BufTmpDeltas.TraPoliza = TmpFoliosSAT.Poliza   
           BufTmpDeltas.TraEndoso = TmpFoliosSAT.Endoso
           BufTmpDeltas.TraIntern = TmpFoliosSAT.FolioInt
           BufTmpDeltas.TraNumFac = TmpFoliosSAT.numfact.
    Run Facturas (2).                                        
    EXPORT DELIMITER "|"  BufTmpDeltas.   
END PROCEDURE.

    
