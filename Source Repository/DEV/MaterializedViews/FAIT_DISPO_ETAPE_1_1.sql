CREATE MATERIALIZED VIEW dev.fait_dispo_etape_1_1 (ref_rsun_phys_oper,ref_rsunite_physique,date_debut_sejour,heure_debut,heure_fin,date_subsequente,date_debut_reserv,date_debut_reserv_web,date_fin_reserv,proflgweb,nbre_nuite_min,nbre_jrs_sans_nuite_min,nbre_nuite_max,indc_fermt,jour_precd_dispo,jour1_capct_dispo,jour2_capct_dispo,jour3_capct_dispo,jour4_capct_dispo,jour5_capct_dispo,jour6_capct_dispo,jour7_capct_dispo,jour8_capct_dispo,jour9_capct_dispo,jour10_capct_dispo,jour11_capct_dispo,jour12_capct_dispo,jour13_capct_dispo,jour14_capct_dispo,jour1_prix_unitr,jour2_prix_unitr,jour3_prix_unitr,jour4_prix_unitr,jour5_prix_unitr,jour6_prix_unitr,jour7_prix_unitr,jour8_prix_unitr,jour9_prix_unitr,jour10_prix_unitr,jour11_prix_unitr,jour12_prix_unitr,jour13_prix_unitr,jour14_prix_unitr,rowid_rsun_phys_oper,rowid_rsunite_min_max,rowid_rs_dispo_date_subsq,rowid_rs_dispo_date_du_jour,fdf_rowid)
REFRESH FAST 
AS SELECT /*+ LEADING (DDJ UMM UPO DDS FDF) */ FDF.ref_rsun_phys_oper,
       FDF.ref_rsunite_physique,
       FDF.date_debut_sejour,
       UMM.ummhredeb,
       UMM.ummhrefin,
       UPO.upodat,  
       NVL(FDF.ummdatdeb,TO_DATE('20000101','YYYYMMDD')),
       NVL(FDF.wdrdat,TO_DATE('20000101','YYYYMMDD')),
       FDF.ummdatfin,
       FDF.proflgweb,
       FDF.uppnuimin,
       FDF.uppnbrjrs,
       FDF.uppnuimax,
       GREATEST (NVL (UPO.upoindfer, 'N'), NVL (FDF.uppindfer, 'N')),
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          -1, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          0, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          1, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          2, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          3, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          4, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          5, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          6, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          7, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          8, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          9, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          10, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          11, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          12, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O'
       THEN 
       DECODE (
          DDS.declg,
          13, DECODE (  SIGN(UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf))),
                1,
                (  UPO.upocapbas
                 - (  ABS (UPO.upoutibascnf)
                    + ABS (UPO.uponbrgelbas)
                    + ABS (UPO.upoutibasnoncnf))),
                    (UPO.uponbrgrpmax
              - (  ABS (UPO.uponbrgrpreecnf)
                 + ABS (UPO.uponbrgrpreegel)
                 + ABS (UPO.uponbrgrpreenoncnf)))),
          NULL) ELSE 0 END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 0, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 1, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 2, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 3, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 4, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 5, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 6, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 7, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 8, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 9, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 10, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 11, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 12, UMM.prix_unitr, NULL) ELSE NULL END,
       CASE WHEN GREATEST (NVL (UPO.upoindfer, 'N'),NVL (UMM.uppindfer, 'N')) <> 'O' AND
                 (UPO.upocapbas - (ABS(UPO.upoutibascnf) + ABS(UPO.uponbrgelbas) + ABS(UPO.upoutibasnoncnf))) > 0 AND
                 (UPO.uponbrgrpmax - (ABS(UPO.uponbrgrpreecnf) + ABS (UPO.uponbrgrpreegel) + ABS (UPO.uponbrgrpreenoncnf))) > 0
                 THEN DECODE (DDS.declg, 13, UMM.prix_unitr, NULL) ELSE NULL END,
       UPO.ROWID,
       UMM.ROWID,
       DDS.ROWID,
       DDJ.ROWID,
       FDF.ROWID
  FROM Rsun_phys_oper UPO,  
       Rsunite_min_max UMM,
       Rs_date_dispo_subsq DDS,
       Rs_date_du_jour DDJ,
       Fait_dispo_filtre FDF
 WHERE DDS.ref_date_dispo = FDF.ref_date_dispo
   AND UPO.ref_rsunite_physique = FDF.ref_rsunite_physique
   AND UPO.upodat = DDS.jour
   AND UPO.upohredeb = FDF.ummhredeb
   AND UPO.upohrefin = FDF.ummhrefin
   AND UMM.ref_rsun_phys_oper = UPO.seqnc
   AND UMM.ref_rsproduit = FDF.ref_rsproduit
   AND NVL(UMM.date_creat,DDJ.jour-1) < DDJ.jour;