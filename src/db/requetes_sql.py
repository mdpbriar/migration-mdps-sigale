from sqlalchemy import text

SQL_MDPS_PROECO = text(f"""
select
matric,
nom,
prenom,
UPPER(sexe) as sexe,
nation,
paynaiss,
lieunaiss,
etatcivil,
--pour l'adresse
ruedomi,
paysdomi,
cpostdomi,
commdomi,
locadomi,
zonedomi,
teldomi,
rueresi,
paysresi,
cpostresi,
commresi,
locaresi,
zoneresi,
telresi,
gsm,
email,
email2,
telbureau,
--matriche,
--reservef,
regnat1 as registre_national_numero, 
datnaiss as date_naissance
from PERSONNE
where regnat1 is not null
and regnat1 != ''
and CHAR_LENGTH(regnat1) = 11
order by matric
""")

SQL_CONTRATS_EN_COURS_PROECO = text("""
select matric
from FONCTION
where DATEFIN >= :date_proeco
or DATEFIN is NULL
""")

SQL_MDPS_SIGALE = text("""
select registre_national_numero, id as personne_id
from personnes.personnes
where est_membre_personnel = true
 and registre_national_numero != ''
and registre_national_numero is not null
""")

SQL_PARAMETER_SIGALE = text("""
select pv.id, pv.code
from core.parameter_values pv
inner join core.parameter_types pt on pv.parameter_type_id = pt.id
where pt.code = :type_parameter
""")

SQL_EMAILS_SIGALE = text("""
select e.id as email_id, e.personne_id, e.valeur, e.email_domaine_id
from personnes.personne_emails e
    inner join personnes.personnes p on p.id = e.personne_id
where p.est_membre_personnel = true
""")