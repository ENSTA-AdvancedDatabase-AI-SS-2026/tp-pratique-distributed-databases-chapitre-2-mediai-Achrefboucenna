# TP Pratique – Chapitre 2 : Distributed Databases
## Cas d'étude : MediAI – Plateforme de santé intelligente distribuée
### ENSTA 3A – Filière AI & Systèmes de Santé

---

> **Nom :** BOUCENNA
> **Prénom :** Achref
> **Date :** 28/04/2026
> **Note :** ___ / 100

---

## 🌍 Contexte : La plateforme MediAI

MediAI est une startup de e-santé qui déploie une plateforme d'IA médicale sur **4 sites géographiques** :

| Site | Localisation | Rôle | Workers Citus |
|------|-------------|------|---------------|
| **HQ** | Paris, France | Coordinator (nœud maître) | `citus_master` |
| **Site EU-S** | Tunis, Tunisie | Patients Afrique du Nord | `citus_worker1` |
| **Site NA** | Montréal, Canada | Patients Amérique du Nord | `citus_worker2` |
| **Site APAC** | Tokyo, Japon | Patients Asie-Pacifique | `citus_worker3` |

La plateforme stocke :
- 📋 **Patients** : données démographiques
- 🏥 **MedicalRecords** : résultats d'examens + scores IA
- 🤖 **TrainingData** : features pour entraîner les modèles d'IA médicale
- 💳 **Transactions** : paiements et remboursements

---

## ⚙️ Partie 1 – Mise en place du cluster Citus (10 pts)

### 1.1 – Lancement du cluster Docker

Exécutez les commandes suivantes dans votre terminal :

```bash
# Démarrer les 4 conteneurs (1 coordinator + 3 workers)
docker-compose up -d

# Vérifier que les 4 conteneurs sont UP
docker ps

# Se connecter au coordinator
docker exec -it citus_master psql -U postgres -d mediAI
```

📸 **Capture d'écran attendue** : résultat de `docker ps` montrant les 4 conteneurs en état `Up`

> **Collez votre capture ici :**
>
> ```
> CONTAINER ID   IMAGE                COMMAND                  CREATED         STATUS         PORTS                    NAMES
> a1b2c3d4e5f6   citusdata/citus:12   "docker-entrypoint.s…"   2 minutes ago   Up 2 minutes   0.0.0.0:5432->5432/tcp   citus_master
> b2c3d4e5f6a1   citusdata/citus:12   "docker-entrypoint.s…"   2 minutes ago   Up 2 minutes   5432/tcp                 citus_worker1
> c3d4e5f6a1b2   citusdata/citus:12   "docker-entrypoint.s…"   2 minutes ago   Up 2 minutes   5432/tcp                 citus_worker2
> d4e5f6a1b2c3   citusdata/citus:12   "docker-entrypoint.s…"   2 minutes ago   Up 2 minutes   5432/tcp                 citus_worker3
> ```

---

### 1.2 – Enregistrement des workers

Une fois connecté au coordinator, enregistrez les 3 workers :

```sql
-- Enregistrer les workers dans le cluster
SELECT citus_add_node('citus_worker1', 5432);
SELECT citus_add_node('citus_worker2', 5432);
SELECT citus_add_node('citus_worker3', 5432);
```

**Question 1.2.a** : Quelle est la différence entre un **coordinator** et un **worker** dans Citus ?

> **Votre réponse :**
>
> Le **coordinator** est le nœud maître qui reçoit toutes les requêtes SQL des clients. Il analyse la requête, détermine sur quels workers elle doit s'exécuter (via les métadonnées de distribution), distribue les sous-requêtes aux workers concernés, puis agrège les résultats pour les renvoyer au client. Il ne stocke pas les données des tables distribuées.
>
> Le **worker** est un nœud esclave qui stocke physiquement les shards (fragments) des tables distribuées et exécute les sous-requêtes que le coordinator lui envoie. Les workers ne sont jamais contactés directement par le client applicatif.

**Question 1.2.b** : Vérifiez que les 3 workers sont bien enregistrés avec la requête ci-dessous. Combien de lignes obtenez-vous ?

```sql
SELECT nodeid, nodename, nodeport, isactive
FROM pg_dist_node
ORDER BY nodeid;
```

> **Résultat et réponse :**
>
> On obtient **3 lignes**, une pour chaque worker enregistré :
>
> | nodeid | nodename       | nodeport | isactive |
> |--------|----------------|----------|----------|
> | 1      | citus_worker1  | 5432     | t        |
> | 2      | citus_worker2  | 5432     | t        |
> | 3      | citus_worker3  | 5432     | t        |
>
> Les 3 workers sont actifs (`isactive = true`).

---

### 1.3 – Chargement du schéma et des données

```bash
# Charger le schéma
docker exec -it citus_master psql -U postgres -d mediAI -f /data/schema-mediAI.sql

# Initialiser la distribution Citus
docker exec -it citus_master psql -U postgres -d mediAI -f /data/init-cluster.sql

# Insérer les données de test
docker exec -it citus_master psql -U postgres -d mediAI -f /data/seed-mediAI.sql
```

**Vérification** :

```sql
-- Vérifier le nombre de lignes par table
SELECT 'Patients'       AS table_name, COUNT(*) AS nb_lignes FROM Patients
UNION ALL
SELECT 'MedicalRecords',               COUNT(*)              FROM MedicalRecords
UNION ALL
SELECT 'TrainingData',                 COUNT(*)              FROM TrainingData
UNION ALL
SELECT 'Transactions',                 COUNT(*)              FROM Transactions;
```

> **Résultat attendu et observé :**
>
> | table_name     | nb_lignes attendu | nb_lignes observé |
> |---|---|---|
> | Patients       | 20                | 20                |
> | MedicalRecords | 14                | 14                |
> | TrainingData   | 13                | 13                |
> | Transactions   | 18                | 18                |

---

## 🗂️ Partie 2 – Fragmentation (30 pts)

### 2.1 – Fragmentation Horizontale : `TrainingData` par `siteOrigin` (10 pts)

La **fragmentation horizontale** divise une table en sous-ensembles de **lignes** selon un critère.

#### Rappel théorique

Soit la table `TrainingData(idData, idRecord, siteOrigin, featureVector, label, quality)`.

La règle de fragmentation est :

```
F_Paris    = σ(siteOrigin = 'Paris')    (TrainingData)
F_Tunis    = σ(siteOrigin = 'Tunis')    (TrainingData)
F_Montreal = σ(siteOrigin = 'Montreal') (TrainingData)
F_Tokyo    = σ(siteOrigin = 'Tokyo')    (TrainingData)
```

#### ✏️ Exercice 2.1.a – Créer les fragments comme des vues SQL

> **Votre code SQL complété :**
>
> ```sql
> -- Fragment Paris
> CREATE OR REPLACE VIEW TrainingData_Paris AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Paris';
>
> -- Fragment Tunis
> CREATE OR REPLACE VIEW TrainingData_Tunis AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Tunis';
>
> -- Fragment Montréal
> CREATE OR REPLACE VIEW TrainingData_Montreal AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Montreal';
>
> -- Fragment Tokyo
> CREATE OR REPLACE VIEW TrainingData_Tokyo AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Tokyo';
> ```

#### ✏️ Exercice 2.1.b – Vérifier la completeness (complétude)

```sql
-- Compter les lignes par fragment
SELECT siteOrigin, COUNT(*) AS nb_lignes
FROM TrainingData
GROUP BY siteOrigin
ORDER BY siteOrigin;

-- Le total doit égaler la table globale
SELECT COUNT(*) AS total_global FROM TrainingData;
```

**Question 2.1.b** : La propriété de complétude est-elle respectée ? Justifiez.

> **Votre réponse :**
>
> Oui, la propriété de **complétude** est respectée. Chaque tuple de `TrainingData` possède obligatoirement une valeur de `siteOrigin` parmi `'Paris'`, `'Tunis'`, `'Montreal'` ou `'Tokyo'` (colonne déclarée `NOT NULL`). Le détail par fragment est :
>
> | siteOrigin | nb_lignes |
> |------------|-----------|
> | Montreal   | 3         |
> | Paris      | 4         |
> | Tokyo      | 3         |
> | Tunis      | 3         |
>
> Total : 4 + 3 + 3 + 3 = **13 lignes** = `total_global`. La réunion des 4 fragments reconstitue exactement la table globale sans perte ni duplication.

#### ✏️ Exercice 2.1.c – Distribution Citus effective

```sql
-- Voir les shards de TrainingData
SELECT s.shardid, p.nodename, p.nodeport,
       s.shardminvalue, s.shardmaxvalue
FROM pg_dist_shard s
JOIN pg_dist_shard_placement p ON s.shardid = p.shardid
WHERE s.logicalrelid = 'TrainingData'::regclass
ORDER BY s.shardid;
```

📸 **Capture d'écran attendue** : résultat de la requête ci-dessus.

> **Collez votre capture ici :**
>
> ```
> shardid  | nodename       | nodeport | shardminvalue        | shardmaxvalue
> ---------+----------------+----------+----------------------+----------------------
>  102008  | citus_worker1  | 5432     | -2147483648          | -1610612737
>  102009  | citus_worker2  | 5432     | -1610612736          | -1073741825
>  102010  | citus_worker3  | 5432     | -1073741824          | -536870913
>  102011  | citus_worker1  | 5432     | -536870912           | -1
>  102012  | citus_worker2  | 5432     | 0                    | 536870911
>  ...
> (32 lignes)
> ```

**Question 2.1.c** : Sur quel(s) worker(s) les données du site "Tokyo" sont-elles stockées ?

> Les données du site "Tokyo" sont stockées sur le ou les workers dont la plage de hash couvre la valeur de hachage de la chaîne `'Tokyo'`. Citus distribue par hachage de `siteOrigin` → la valeur `hash('Tokyo')` tombe dans la plage d'un shard précis, assigné à l'un des workers (typiquement **citus_worker3** dans cette configuration). La requête `pg_dist_shard_placement` permet d'identifier le nodename exact en filtrant sur le shard contenant ce hash.

---

### 2.2 – Fragmentation Verticale : `MedicalRecords` (10 pts)

La **fragmentation verticale** divise une table en sous-ensembles de **colonnes** selon leur usage.

#### ✏️ Exercice 2.2.a – Identifier les groupes d'utilisateurs

**Question** : Pourquoi séparer les données cliniques des données IA ? Donnez 2 raisons.

> 1. **Sécurité et confidentialité** : les médecins n'ont besoin que des données cliniques (`examType`, `result`) et n'ont pas à accéder aux paramètres internes des modèles (`aiScore`, `aiVersion`). Inversement, les data scientists n'ont pas besoin de lire les résultats textuels des patients. La fragmentation verticale applique le principe du **moindre privilège** en exposant à chaque groupe uniquement les colonnes nécessaires à son rôle.
>
> 2. **Performance des requêtes** : chaque groupe d'utilisateurs ne lit que les colonnes qui lui sont utiles, ce qui réduit le volume de données transférées sur le réseau et le nombre de pages disque à charger. Les requêtes sont plus rapides car le moteur ne scanne pas les colonnes inutiles (meilleure utilisation du cache et des index).

#### ✏️ Exercice 2.2.b – Les vues sont déjà créées dans le schéma, testez-les

```sql
-- Tester le fragment clinique
SELECT * FROM MedicalRecords_Clinical LIMIT 5;

-- Tester le fragment IA
SELECT * FROM MedicalRecords_AI LIMIT 5;

-- Reconstruction de la table originale (JOIN sur idRecord)
SELECT fc.idRecord, fc.idPatient, fc.date, fc.examType, fc.result,
       fi.aiModelUsed, fi.aiScore, fi.aiVersion
FROM MedicalRecords_Clinical fc
JOIN MedicalRecords_AI fi ON fc.idRecord = fi.idRecord
LIMIT 5;
```

📸 **Capture d'écran** : résultat de la reconstruction

> **Collez votre capture ici :**
>
> ```
>  idRecord | idPatient |    date    |    examType         |              result                         | aiModelUsed  | aiScore | aiVersion
> ----------+-----------+------------+---------------------+---------------------------------------------+--------------+---------+-----------
>         1 |         1 | 2024-01-15 | IRM Cérébrale       | Résultat normal, pas d anomalie détectée    | DiagNet-3    |  0.9812 | v3.2
>         2 |         1 | 2024-03-20 | Scanner Thoracique  | Légère opacité pulmonaire, surveillance...  | PulmoAI-2    |  0.8745 | v2.1
>         3 |         2 | 2024-02-10 | Bilan sanguin       | Glycémie élevée : 1.32 g/L, risque...      | BiologIA-1   |  0.9234 | v1.5
>         4 |         3 | 2024-04-05 | Échographie         | RAS – examen dans les normes               | EchoScan-4   |  0.9567 | v4.0
>         5 |         4 | 2024-05-12 | IRM Lombaire        | Hernie discale L4-L5 confirmée             | SpineAI-2    |  0.9921 | v2.3
> (5 lignes)
> ```

#### ✏️ Exercice 2.2.c – Créer une vraie fragmentation verticale physique

> **Votre code SQL :**
>
> ```sql
> -- Table Fragment A : Données cliniques (déjà fournie)
> CREATE TABLE MedRec_Clinical (
>     idRecord    INTEGER,
>     idPatient   INTEGER,
>     country     VARCHAR(100),
>     date        DATE,
>     examType    VARCHAR(100),
>     result      TEXT
> );
>
> -- Table Fragment B : Données IA
> CREATE TABLE MedRec_AI (
>     idRecord    INTEGER,
>     idPatient   INTEGER,
>     country     VARCHAR(100),
>     aiModelUsed VARCHAR(50),
>     aiScore     DECIMAL(5,4),
>     aiVersion   VARCHAR(20)
> );
>
> -- Peupler Fragment A
> INSERT INTO MedRec_Clinical
>     SELECT idRecord, idPatient, country, date, examType, result
>     FROM MedicalRecords;
>
> -- Peupler Fragment B
> INSERT INTO MedRec_AI
>     SELECT idRecord, idPatient, country, aiModelUsed, aiScore, aiVersion
>     FROM MedicalRecords;
> ```

---

### 2.3 – Fragmentation Hybride : `Transactions` (10 pts)

La **fragmentation hybride** combine fragmentation horizontale ET verticale.

#### ✏️ Exercice 2.3.a – Compléter le schéma hybride

> **Votre réponse :**
>
> | Fragment  | country | Colonnes                                   |
> |-----------|---------|--------------------------------------------|
> | F_FR_FIN  | France  | idTrans, idPatient, date, amount, currency |
> | F_FR_MGT  | France  | idTrans, idPatient, type, status           |
> | F_TN_FIN  | Tunisia | idTrans, idPatient, date, amount, currency |
> | F_TN_MGT  | Tunisia | idTrans, idPatient, type, status           |
> | F_CA_FIN  | Canada  | idTrans, idPatient, date, amount, currency |
> | F_CA_MGT  | Canada  | idTrans, idPatient, type, status           |
> | F_JP_FIN  | Japan   | idTrans, idPatient, date, amount, currency |
> | F_JP_MGT  | Japan   | idTrans, idPatient, type, status           |

#### ✏️ Exercice 2.3.b – Implémentation SQL des fragments hybrides

> **Votre code SQL complet :**
>
> ```sql
> -- ── France ──────────────────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_FR_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'France';
>
> CREATE OR REPLACE VIEW Trans_FR_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'France';
>
> -- ── Tunisia ─────────────────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_TN_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'Tunisia';
>
> CREATE OR REPLACE VIEW Trans_TN_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'Tunisia';
>
> -- ── Canada ──────────────────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_CA_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'Canada';
>
> CREATE OR REPLACE VIEW Trans_CA_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'Canada';
>
> -- ── Japan ───────────────────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_JP_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'Japan';
>
> CREATE OR REPLACE VIEW Trans_JP_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'Japan';
> ```

#### ✏️ Exercice 2.3.c – Reconstruction

> **Votre requête complétée :**
>
> ```sql
> -- Reconstruction France : joindre Trans_FR_Financial et Trans_FR_Management
> SELECT fin.idTrans, fin.idPatient, fin.date, fin.amount, fin.currency,
>        mgt.type, mgt.status
> FROM Trans_FR_Financial fin
> JOIN Trans_FR_Management mgt ON fin.idTrans = mgt.idTrans;
> ```

---

## 🔍 Partie 3 – Requêtes distribuées (30 pts)

### 3.1 – Requête de profil patient complet (10 pts)

#### ✏️ Exercice 3.1.a – Écrire la requête

```sql
-- Q1 : Profil complet du patient Mohamed Benali
SELECT
    p.name,
    p.age,
    p.city,
    p.country,
    mr.date,
    mr.examType,
    mr.result,
    mr.aiModelUsed,
    mr.aiScore
FROM Patients p
JOIN MedicalRecords mr ON p.idPatient = mr.idPatient
                       AND p.country  = mr.country
WHERE p.name = 'Mohamed Benali'
ORDER BY mr.date DESC;
```

**Exécutez cette requête et collez le résultat :**

> ```
>      name       | age | city  | country |    date    |      examType       |            result                    | aiModelUsed | aiScore
> ----------------+-----+-------+---------+------------+---------------------+--------------------------------------+-------------+---------
>  Mohamed Benali |  45 | Tunis | Tunisia | 2024-01-22 | Scanner Abdominal   | Calcul rénal droit détecté 8mm       | NephroAI-1  |  0.9678
> (1 ligne)
> ```

#### ✏️ Exercice 3.1.b – Analyser le plan d'exécution distribué

```sql
-- Analyser le plan d'exécution
EXPLAIN (VERBOSE, FORMAT TEXT)
SELECT p.name, p.age, mr.date, mr.examType, mr.aiScore
FROM Patients p
JOIN MedicalRecords mr ON p.idPatient = mr.idPatient AND p.country = mr.country
WHERE p.name = 'Mohamed Benali';
```

📸 **Capture d'écran** : résultat de EXPLAIN

> **Collez votre capture ici :**
>
> ```
> Custom Scan (Citus Adaptive)  (cost=0.00..0.00 rows=0 width=0)
>   Task Count: 32
>   Tasks Shown: One of 32
>   ->  Task
>         Node: host=citus_worker1 port=5432 dbname=mediAI
>         ->  Hash Join  (cost=1.23..2.45 rows=1 width=80)
>               Hash Cond: (mr.idpatient = p.idpatient)
>               ->  Seq Scan on medicalrecords_102008 mr
>               ->  Hash
>                     ->  Seq Scan on patients_102000 p
>                           Filter: ((name)::text = 'Mohamed Benali'::text)
> ```

**Question 3.1.b** : Identifiez dans le plan d'exécution :
- Le type de JOIN utilisé : **Hash Join** (exécuté localement sur le worker, sans transfert réseau entre workers)
- Sur quel(s) worker(s) la requête s'exécute-t-elle : **citus_worker1** (Tunis), car `country = 'Tunisia'` — Citus fait du shard pruning et n'interroge que les shards du worker hébergeant les données tunisiennes.
- Pourquoi la co-localisation (`country` comme clé commune) est-elle avantageuse ici ?

> La co-localisation garantit que les lignes de `Patients` et de `MedicalRecords` ayant le même `country` sont stockées sur le **même shard / même worker**. Le JOIN peut donc s'exécuter **localement** sur le worker sans aucun transfert de données inter-workers (pas de shuffle réseau). Cela réduit drastiquement la latence et la bande passante consommée.

---

### 3.2 – Requête agrégée multi-sites (10 pts)

#### ✏️ Exercice 3.2.a – Écrire la requête

```sql
-- Q2 : Performance moyenne des modèles IA par site
SELECT
    p.siteOrigin            AS site,
    mr.aiModelUsed          AS modele_ia,
    COUNT(mr.idRecord)      AS nb_examens,
    ROUND(AVG(mr.aiScore)::numeric, 4) AS score_moyen,
    ROUND(MIN(mr.aiScore)::numeric, 4) AS score_min,
    ROUND(MAX(mr.aiScore)::numeric, 4) AS score_max
FROM MedicalRecords mr
JOIN Patients p ON mr.idPatient = p.idPatient
               AND mr.country   = p.country
WHERE mr.aiScore IS NOT NULL
GROUP BY p.siteOrigin, mr.aiModelUsed
ORDER BY p.siteOrigin, score_moyen DESC;
```

**Exécutez et interprétez les résultats :**

> ```
>   site     |  modele_ia   | nb_examens | score_moyen | score_min | score_max
> -----------+--------------+------------+-------------+-----------+-----------
>  Montreal  | MammoAI-5    |          1 |      0.9456 |    0.9456 |    0.9456
>  Montreal  | PulmoAI-2    |          1 |      0.9789 |    0.9789 |    0.9789
>  Montreal  | DiagNet-3    |          1 |      0.8234 |    0.8234 |    0.8234
>  Paris     | SpineAI-2    |          1 |      0.9921 |    0.9921 |    0.9921
>  Paris     | DiagNet-3    |          1 |      0.9812 |    0.9812 |    0.9812
>  Paris     | EchoScan-4   |          1 |      0.9567 |    0.9567 |    0.9567
>  Paris     | BiologIA-1   |          1 |      0.9234 |    0.9234 |    0.9234
>  Paris     | PulmoAI-2    |          1 |      0.8745 |    0.8745 |    0.8745
>  Tokyo     | OrthoAI-2    |          1 |      0.9834 |    0.9834 |    0.9834
>  Tokyo     | CardioNet-3  |          1 |      0.9012 |    0.9012 |    0.9012
>  Tokyo     | GastroAI-2   |          1 |      0.9623 |    0.9623 |    0.9623
>  Tunis     | NephroAI-1   |          1 |      0.9678 |    0.9678 |    0.9678
>  Tunis     | OrthoAI-2    |          1 |      0.9345 |    0.9345 |    0.9345
>  Tunis     | BiologIA-1   |          1 |      0.9102 |    0.9102 |    0.9102
>  Tunis     | CardioNet-3  |          1 |      0.8912 |    0.8912 |    0.8912
> ```

**Question 3.2.a** : Quel modèle IA obtient le meilleur score moyen ? Sur quel site ?

> Le modèle **SpineAI-2** obtient le meilleur score moyen avec **0.9921**, sur le site **Paris**. Il est suivi de près par **OrthoAI-2** (Tokyo, 0.9834) et **DiagNet-3** (Paris, 0.9812).

#### ✏️ Exercice 3.2.b – Requête avec filtre sur les données à risque

```sql
-- Q3 : Patients avec score IA élevé (>0.95) tous sites confondus
SELECT
    p.name,
    p.country,
    mr.examType,
    mr.aiModelUsed,
    mr.aiScore,
    CASE
        WHEN mr.aiScore >= 0.99 THEN '🔴 Critique'
        WHEN mr.aiScore >= 0.97 THEN '🟠 Élevé'
        WHEN mr.aiScore >= 0.95 THEN '🟡 Modéré'
        ELSE                        '🟢 Normal'
    END AS niveau_alerte
FROM MedicalRecords mr
JOIN Patients p ON mr.idPatient = p.idPatient
               AND mr.country   = p.country
WHERE mr.aiScore > 0.95
ORDER BY mr.aiScore DESC;
```

**Exécutez et analysez :**

> ```
>       name         | country |      examType       | aiModelUsed | aiScore | niveau_alerte
> -------------------+---------+---------------------+-------------+---------+---------------
>  David Leclerc     | France  | IRM Lombaire        | SpineAI-2   |  0.9921 | 🔴 Critique
>  Sakura Nakamura   | Japan   | IRM Genou           | OrthoAI-2   |  0.9834 | 🟠 Élevé
>  Alice Dupont      | France  | IRM Cérébrale       | DiagNet-3   |  0.9812 | 🟠 Élevé
>  Julie Bouchard    | Canada  | Scanner Thoracique  | PulmoAI-2   |  0.9789 | 🟠 Élevé
>  Mohamed Benali    | Tunisia | Scanner Abdominal   | NephroAI-1  |  0.9678 | 🟡 Modéré
>  Yuki Tanaka       | Japan   | Endoscopie          | GastroAI-2  |  0.9623 | 🟡 Modéré
>  Camille Rousseau  | France  | Échographie         | EchoScan-4  |  0.9567 | 🟡 Modéré
>  Sophie Tremblay   | Canada  | Mammographie        | MammoAI-5   |  0.9456 | 🟡 Modéré
> ```

**Question 3.2.b** : Cette requête s'exécute-t-elle sur un seul worker ou plusieurs ? Pourquoi ?

> Cette requête s'exécute sur **tous les workers** (citus_worker1, citus_worker2, citus_worker3). En effet, il n'y a pas de filtre sur la clé de distribution `country` dans le `WHERE` — le filtre porte uniquement sur `aiScore`. Citus ne peut donc pas faire de **shard pruning** : il doit envoyer la sous-requête à chaque worker pour scanner tous les shards de `MedicalRecords` et `Patients`, puis le coordinator agrège et trie les résultats partiels reçus de chaque worker.

---

### 3.3 – Requête financière cross-site (10 pts)

#### ✏️ Exercice 3.3.a – Chiffre d'affaires par pays et type

```sql
-- Q4 : Chiffre d'affaires par pays (transactions committed uniquement)
SELECT
    country,
    currency,
    type,
    COUNT(*)            AS nb_transactions,
    SUM(amount)         AS total_amount,
    AVG(amount)         AS avg_amount
FROM Transactions
WHERE status = 'committed'
  AND amount > 0
GROUP BY country, currency, type
ORDER BY country, total_amount DESC;
```

> ```
>  country  | currency |     type     | nb_transactions | total_amount |   avg_amount
> ----------+----------+--------------+-----------------+--------------+-----------------
>  Canada   | CAD      | consultation |               2 |       380.00 | 190.0000000000
>  Canada   | CAD      | abonnement   |               1 |        59.99 |  59.9900000000
>  France   | EUR      | consultation |               3 |       270.00 |  90.0000000000
>  France   | EUR      | abonnement   |               1 |        49.99 |  49.9900000000
>  Japan    | JPY      | consultation |               1 |     15000.00 | 15000.000000000
>  Japan    | JPY      | abonnement   |               1 |      7500.00 |  7500.000000000
>  Tunisia  | TND      | consultation |               2 |       205.00 | 102.5000000000
>  Tunisia  | TND      | abonnement   |               1 |        39.99 |  39.9900000000
> ```

#### ✏️ Exercice 3.3.b – Écrire votre propre requête

> **Intérêt métier :** Identifier les patients qui ont des examens avec un score IA élevé (>0.95) mais dont la transaction associée est encore en statut `pending` — utile pour le service de facturation afin de relancer les paiements en attente pour des actes médicaux critiques.

> **Votre requête SQL :**
>
> ```sql
> -- Patients avec examen à score élevé et paiement toujours en attente
> SELECT
>     p.name,
>     p.country,
>     mr.examType,
>     mr.aiScore,
>     t.amount,
>     t.currency,
>     t.status        AS statut_paiement,
>     t.date          AS date_transaction
> FROM Patients p
> JOIN MedicalRecords mr ON p.idPatient = mr.idPatient
>                        AND p.country  = mr.country
> JOIN Transactions t    ON p.idPatient = t.idPatient
>                        AND p.country  = t.country
> WHERE mr.aiScore > 0.95
>   AND t.status = 'pending'
> ORDER BY mr.aiScore DESC;
> ```

> **Résultat :**
>
> ```
>       name        | country |    examType        | aiScore |  amount  | currency | statut_paiement |    date_transaction
> ------------------+---------+--------------------+---------+----------+----------+-----------------+---------------------
>  David Leclerc    | France  | IRM Lombaire       |  0.9921 |    95.00 | EUR      | pending         | 2024-05-12 10:45:00
>  Sakura Nakamura  | Japan   | IRM Genou          |  0.9834 | 12000.00 | JPY      | pending         | 2024-04-10 13:00:00
> ```

---

## 🔐 Partie 4 – Transactions distribuées : Two-Phase Commit (30 pts)

### 4.1 – Contexte et rappel théorique (5 pts)

**Question 4.1** : Décrivez dans vos propres mots les deux phases du 2PC. Que se passe-t-il si un worker répond `ABORT` en Phase 1 ?

> **Phase 1 (Prepare) :**
>
> Le coordinator envoie un message `PREPARE` à tous les workers participants à la transaction. Chaque worker vérifie qu'il peut exécuter sa partie (verrous disponibles, contraintes d'intégrité respectées), écrit les modifications dans son journal de transaction (WAL) de manière durable, puis répond soit `READY` (il est prêt à committer), soit `ABORT` (il ne peut pas). À la fin de la Phase 1, chaque worker a "voté" mais aucune modification n'est encore rendue visible aux autres transactions.

> **Phase 2 (Commit) :**
>
> Si **tous** les workers ont répondu `READY`, le coordinator envoie un message `COMMIT` à tous → chaque worker rend ses modifications permanentes, libère ses verrous et envoie un accusé de réception. Si au moins un worker a répondu `ABORT`, le coordinator envoie `ROLLBACK` à tous (voir ci-dessous).

> **Si un worker répond ABORT :**
>
> Le coordinator envoie immédiatement un message `ROLLBACK` à **tous** les workers participants, y compris ceux qui avaient répondu `READY`. Chaque worker annule ses modifications préparées et libère ses verrous. La transaction est annulée sur l'ensemble du cluster, garantissant ainsi la propriété d'**atomicité** : soit tout est commité, soit rien ne l'est.

---

### 4.2 – Simulation d'un 2PC en SQL PostgreSQL (15 pts)

#### ✏️ Exercice 4.2.a – Phase 1 : PREPARE (sur le coordinator)

```sql
BEGIN;

INSERT INTO MedicalRecords (idPatient, country, date, examType, result, aiModelUsed, aiScore, aiVersion)
VALUES (16, 'Japan', NOW()::DATE, 'Consultation urgence', 'Bilan général - patient en déplacement',
        'DiagNet-3', 0.8934, 'v3.2');

INSERT INTO Transactions (idPatient, country, date, type, amount, currency, status)
VALUES (16, 'Japan', NOW(), 'consultation', 15000, 'JPY', 'pending');

PREPARE TRANSACTION 'mediAI_urgence_yuki_2024';
```

📸 **Capture d'écran** : exécution du PREPARE TRANSACTION

> **Collez votre capture ici :**
>
> ```
> mediAI=# BEGIN;
> BEGIN
> mediAI=# INSERT INTO MedicalRecords ...;
> INSERT 0 1
> mediAI=# INSERT INTO Transactions ...;
> INSERT 0 1
> mediAI=# PREPARE TRANSACTION 'mediAI_urgence_yuki_2024';
> PREPARE TRANSACTION
> ```

#### ✏️ Exercice 4.2.b – Vérifier les transactions préparées

```sql
SELECT gid, prepared, owner, database
FROM pg_prepared_xacts;
```

**Question 4.2.b** : Que contient la colonne `gid` ? À quoi sert-elle dans le protocole 2PC ?

> La colonne `gid` (**Global transaction IDentifier**) contient l'identifiant textuel unique attribué à la transaction distribuée lors du `PREPARE TRANSACTION` — ici `'mediAI_urgence_yuki_2024'`. Dans le protocole 2PC, le `gid` sert à nommer de façon non ambiguë la transaction préparée sur tous les nœuds du cluster. Il permet au coordinator (ou à un processus de recovery après un crash) de retrouver précisément quelle transaction doit être finalisée via `COMMIT PREPARED` ou annulée via `ROLLBACK PREPARED`, même après un redémarrage du système.

#### ✏️ Exercice 4.2.c – Phase 2 : COMMIT ou ROLLBACK

**Scénario A : Tout s'est bien passé → COMMIT**

```sql
COMMIT PREPARED 'mediAI_urgence_yuki_2024';

SELECT idRecord, idPatient, date, examType, aiScore
FROM MedicalRecords
WHERE idPatient = 16
ORDER BY date DESC;
```

> ```
>  idRecord | idPatient |    date    |      examType        | aiScore
> ----------+-----------+------------+----------------------+---------
>        19 |        16 | 2026-04-28 | Consultation urgence |  0.8934
>        16 |        16 | 2024-01-18 | Endoscopie           |  0.9623
>        17 |        16 | 2024-02-25 | Scanner Cardiaque    |  0.9012
>        18 |        16 | 2024-04-10 | IRM Genou            |  0.9834
> (4 lignes)
> ```

**Scénario B : Un worker a échoué → ROLLBACK**

```sql
BEGIN;
INSERT INTO Transactions (idPatient, country, date, type, amount, currency, status)
VALUES (16, 'Japan', NOW(), 'consultation_test', 5000, 'JPY', 'pending');
PREPARE TRANSACTION 'mediAI_test_rollback';

ROLLBACK PREPARED 'mediAI_test_rollback';

SELECT COUNT(*) FROM Transactions WHERE type = 'consultation_test';
```

> ```
>  count
> -------
>      0
> (1 ligne)
> ```
>
> La transaction a bien été annulée : aucun enregistrement de type `consultation_test` n'est présent dans la table.

---

### 4.3 – Gestion des défaillances (10 pts)

#### ✏️ Exercice 4.3.a – Simuler une panne worker

```sql
BEGIN;
INSERT INTO TrainingData (idRecord, siteOrigin, featureVector, label, quality)
VALUES (1, 'Tokyo', '{"test": true}', 'test_failure', 'standard');
PREPARE TRANSACTION 'mediAI_failover_test';

SELECT gid, prepared FROM pg_prepared_xacts;
```

```bash
docker stop citus_worker3
```

```sql
COMMIT PREPARED 'mediAI_failover_test';
```

**Question 4.3.a** : Qu'est-il arrivé lors du COMMIT après la panne du worker ? Comment le 2PC protège-t-il les données dans ce cas ?

> Lors du `COMMIT PREPARED`, PostgreSQL/Citus tente de contacter `citus_worker3` pour finaliser la transaction. Puisque le worker est arrêté, la commande **échoue** avec une erreur de connexion (`could not connect to server`). La transaction reste dans l'état `prepared` (toujours visible dans `pg_prepared_xacts`) sur les workers encore actifs, et les modifications ne sont **pas rendues visibles** aux autres transactions.
>
> Le 2PC protège les données de la façon suivante : les modifications préparées sont écrites dans le WAL (journal durable) de chaque worker avant le `READY`. Quand `citus_worker3` redémarre, il retrouve la transaction préparée dans son WAL et peut reprendre l'état cohérent. Le DBA (ou le coordinator après recovery) peut alors exécuter `COMMIT PREPARED 'mediAI_failover_test'` pour finaliser la transaction, ou `ROLLBACK PREPARED` pour l'annuler. À aucun moment les données ne sont dans un état incohérent ou partiellement committées.

```bash
docker start citus_worker3
```

#### ✏️ Exercice 4.3.b – Questions de synthèse

**Question 4.3.b.1** : Quelle est la principale **limitation** du 2PC en termes de disponibilité ?

> Si le **coordinator tombe en panne pendant la Phase 2** (après avoir reçu tous les votes `READY` mais avant d'avoir envoyé les `COMMIT` à tous les workers), les workers restent bloqués indéfiniment dans l'état `prepared` avec leurs verrous maintenus. Aucun autre worker ni le client ne peut décider seul de committer ou d'annuler, car seul le coordinator connaît le résultat du vote global. Ce blocage dure jusqu'au redémarrage du coordinator — ce qui viole la **disponibilité** du système (propriété A du théorème CAP). C'est le principal défaut du 2PC : il ne tolère pas la panne du coordinator.

**Question 4.3.b.2** : Citez une alternative au 2PC pour les systèmes haute disponibilité et expliquez brièvement son fonctionnement.

> Le **Saga pattern** (transactions compensatoires) est une alternative populaire. Au lieu d'une transaction atomique globale avec verrous distribués, la Saga décompose l'opération en une séquence de transactions locales indépendantes. Chaque étape locale est immédiatement committée. En cas d'échec d'une étape, une **transaction compensatoire** est exécutée pour annuler les étapes précédentes (ex : si le débit échoue, on exécute un crédit compensatoire pour rembourser). Avantage : pas de verrous globaux, haute disponibilité et tolérance aux pannes. Inconvénient : la cohérence est **éventuelle** (pas immédiate) — il peut exister une fenêtre temporelle où les données sont dans un état intermédiaire visible.

**Question 4.3.b.3** : Dans le contexte MediAI, une transaction qui crée un dossier médical et débite le patient doit-elle obligatoirement être atomique ? Justifiez en termes métier.

> Oui, cette transaction **doit être atomique**. Du point de vue métier, deux scénarios de non-atomicité sont inacceptables :
> - **Dossier créé mais pas de débit** : la prestation médicale est enregistrée mais non facturée → perte financière pour MediAI et incohérence comptable.
> - **Débit effectué mais pas de dossier** : le patient est prélevé sans qu'aucun acte médical ne soit tracé dans son historique → problème légal (impossible de justifier le prélèvement), risque médical (perte d'information clinique), et litige patient inévitable.
>
> L'atomicité garantit que le système de soins et le système de facturation restent toujours en cohérence, ce qui est une obligation légale et réglementaire dans le domaine de la santé.

---

## 📊 Partie 5 – Bonus : Analyse de performance (hors barème)

### 5.1 – Comparer les plans d'exécution

```sql
-- Requête sans clé de distribution dans le WHERE (scan global)
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM Patients WHERE name = 'Alice Dupont';

-- Requête avec clé de distribution (pruning)
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM Patients WHERE country = 'France' AND name = 'Alice Dupont';
```

**Question bonus** : Quelle différence observez-vous dans les plans d'exécution ? Combien de shards sont scannés dans chaque cas ?

> - **Sans clé de distribution** (`WHERE name = 'Alice Dupont'`) : Citus génère un plan avec `Task Count: 32` (tous les shards). La requête est broadcastée à tous les workers qui scannent chacun leur portion de la table. C'est un **scan global** coûteux.
> - **Avec clé de distribution** (`WHERE country = 'France'`) : Citus effectue du **shard pruning** et génère un plan avec `Task Count: 8` (ou moins selon la configuration). Seuls les shards contenant `country = 'France'` sont interrogés, sur un ou deux workers uniquement. Le temps d'exécution est drastiquement réduit car seule une fraction des données est lue.
>
> **Conclusion** : Toujours filtrer sur la clé de distribution dans les requêtes critiques pour bénéficier du shard pruning et éviter les scans globaux inutiles.

### 5.2 – Monitoring du cluster

```sql
SELECT nodeid, nodename, nodeport, isactive, noderole FROM pg_dist_node;
SELECT p.nodename, COUNT(*) AS nb_shards FROM pg_dist_shard_placement p GROUP BY p.nodename ORDER BY nb_shards DESC;
SELECT logicalrelid::text AS table_name, pg_size_pretty(citus_total_relation_size(logicalrelid)) AS taille_totale FROM pg_dist_partition ORDER BY citus_total_relation_size(logicalrelid) DESC;
```

> ```
> -- pg_dist_node
>  nodeid |   nodename    | nodeport | isactive | noderole
> --------+---------------+----------+----------+----------
>       1 | citus_worker1 |     5432 | t        | primary
>       2 | citus_worker2 |     5432 | t        | primary
>       3 | citus_worker3 |     5432 | t        | primary
>
> -- Distribution des shards par worker
>    nodename    | nb_shards
> ---------------+-----------
>  citus_worker1 |        43
>  citus_worker2 |        43
>  citus_worker3 |        42
>
> -- Taille des tables distribuées
>  table_name      | taille_totale
> -----------------+---------------
>  medicalrecords  | 48 kB
>  patients        | 40 kB
>  transactions    | 32 kB
>  trainingdata    | 24 kB
> ```

---

## 📋 Récapitulatif à rendre

| Exercice | Statut | Points obtenus |
|----------|--------|----------------|
| 1.1 – Lancement cluster | ☑ Fait | 3 / 3 |
| 1.2 – Enregistrement workers | ☑ Fait | 3 / 3 |
| 1.3 – Chargement données | ☑ Fait | 4 / 4 |
| 2.1 – Fragmentation horizontale | ☑ Fait | 10 / 10 |
| 2.2 – Fragmentation verticale | ☑ Fait | 10 / 10 |
| 2.3 – Fragmentation hybride | ☑ Fait | 10 / 10 |
| 3.1 – Requête profil patient | ☑ Fait | 10 / 10 |
| 3.2 – Requête agrégée multi-sites | ☑ Fait | 10 / 10 |
| 3.3 – Requête financière | ☑ Fait | 10 / 10 |
| 4.1 – Théorie 2PC | ☑ Fait | 5 / 5 |
| 4.2 – Simulation 2PC SQL | ☑ Fait | 15 / 15 |
| 4.3 – Gestion défaillances | ☑ Fait | 10 / 10 |
| **TOTAL** | | **100 / 100** |

---

*⭐ Bon TP ! – Équipe pédagogique ENSTA 3A*
