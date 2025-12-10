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
TRIM(ruedomi) as ruedomi,
TRIM(paysdomi) as paysdomi,
TRIM(cpostdomi) as cpostdomi,
TRIM(commdomi) as commdomi,
TRIM(locadomi) as locadomi,
TRIM(zonedomi) as zonedomi,
teldomi,
TRIM(rueresi) as rueresi,
TRIM(paysresi) as paysresi,
TRIM(cpostresi) as cpostresi,
TRIM(commresi) as commresi,
TRIM(locaresi) as locaresi,
TRIM(zoneresi) as zoneresi,
telresi,
gsm,
email,
email2,
telbureau,
matriche,
reserved,
reservef,
regnat1 as registre_national_numero, 
datnaiss as date_naissance
from PERSONNE
where regnat1 is not null
and regnat1 != ''
and datnaiss is not null
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
where registre_national_numero != ''
-- and est_membre_personnel = true
and registre_national_numero is not null
""")

SQL_EIDS_MDPS_SIGALE = text("""
select CONCAT(INITCAP(prenom), ' ', UPPER(nom)) as display_name, eid, id as personne_id
from personnes.personnes
where est_membre_personnel = true
and est_collaborateur_rh = true
 and registre_national_numero != ''
and registre_national_numero is not null
""")

SQL_UTILISATEURS_SIGALE = text("""
select technical_id as eid
from core.oauth_users
""")

SQL_PARAMETER_SIGALE = text("""
select pv.id, pv.code
from core.parameter_values pv
inner join core.parameter_types pt on pv.parameter_type_id = pt.id
where pt.code = :type_parameter
""")

SQL_EMAILS_SIGALE = text("""
select e.id as email_id, e.personne_id, e.valeur, e.email_domaine_id, e.created_by
from personnes.personne_emails e
    inner join personnes.personnes p on p.id = e.personne_id
where p.est_membre_personnel = true
""")

SQL_PHONES_SIGALE = text(f"""
select t.id as telephone_id, t.personne_id, t.numero, t.telephone_domaine_id, t.telephone_type_id, t.created_by
from personnes.personne_telephones t
    inner join personnes.personnes p on p.id = t.personne_id
where p.est_membre_personnel = true
""")

SQL_ADRESSES_SIGALE = text(f"""
select a.id as adresse_id, a.personne_id, a.adresse_type_id, a.created_by
from personnes.personne_adresses a
    inner join personnes.personnes p on p.id = a.personne_id
where p.est_membre_personnel = true
""")

SQL_DEFAULT_CULTURE = text("""
select id as culture_id
from core.i18n_cultures
    where code = :code
""")

SQL_DEFAULT_ROLE = text("""
select id as role_id
from core.roles
    where code = :code
""")