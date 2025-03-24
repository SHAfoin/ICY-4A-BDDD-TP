-- 2.3 CASSANDRA
-- Créer keyspace
CREATE KEYSPACE tp2_cassandra
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE tp2_cassandra;

-- Créer les tables
CREATE TABLE utilisateurs (
username TEXT PRIMARY KEY,
password TEXT,
);

CREATE TABLE following (
username TEXT,
followed TEXT,
PRIMARY KEY (username, followed),
);

CREATE TABLE followers (
username TEXT,
following TEXT,
PRIMARY KEY (username, following),
);

CREATE TABLE shouts (
shout_id UUID PRIMARY KEY,
username TEXT,
body TEXT,
);

CREATE TABLE usershouts (
username TEXT,
usershout_id UUID,
body TEXT,
PRIMARY KEY (username, usershout_id),
);

CREATE TABLE shoutwall (
username TEXT,
shoutwall_id UUID,
posted_by TEXT,
body TEXT,
PRIMARY KEY (username, shoutwall_id),
);

-- 2.3 CASSANDRA
-- Insérer des données
INSERT INTO utilisateurs (username, password) VALUES 
('homer', 'donuts');
INSERT INTO utilisateurs (username, password) VALUES 
('marge', 'family');
INSERT INTO utilisateurs (username, password) VALUES 
('bart', 'skateboard');
INSERT INTO utilisateurs (username, password) VALUES 
('lisa', 'saxophone');
INSERT INTO utilisateurs (username, password) VALUES 
('maggie', 'pacifier');

INSERT INTO following (username, followed) VALUES 
('homer', 'marge');
INSERT INTO following (username, followed) VALUES 
('homer', 'bart');
INSERT INTO following (username, followed) VALUES 
('marge', 'lisa');
INSERT INTO following (username, followed) VALUES 
('bart', 'lisa');
INSERT INTO following (username, followed) VALUES 
('lisa', 'maggie');

INSERT INTO followers (username, following) VALUES 
('marge', 'homer');
INSERT INTO followers (username, following) VALUES 
('bart', 'homer');
INSERT INTO followers (username, following) VALUES 
('lisa', 'marge');
INSERT INTO followers (username, following) VALUES 
('lisa', 'bart');
INSERT INTO followers (username, following) VALUES 
('maggie', 'lisa');

INSERT INTO shouts (shout_id, username, body) VALUES
(uuid(), 'homer', 'Mmm... donuts');
INSERT INTO shouts (shout_id, username, body) VALUES
(uuid(), 'marge', 'Time to clean the house!');
INSERT INTO shouts (shout_id, username, body) VALUES
(uuid(), 'bart', 'Eat my shorts!');
INSERT INTO shouts (shout_id, username, body) VALUES
(uuid(), 'lisa', 'Playing my saxophone.');
INSERT INTO shouts (shout_id, username, body) VALUES
(uuid(), 'maggie', 'Goo goo ga ga!');

INSERT INTO usershouts (username, usershout_id, body) VALUES
('homer', uuid(),  'Mmm... donuts');
INSERT INTO usershouts (username, usershout_id, body) VALUES
('marge', uuid(),  'Time to clean the house!');
INSERT INTO usershouts (username, usershout_id, body) VALUES
('bart', uuid(),  'Eat my shorts!');
INSERT INTO usershouts (username, usershout_id, body) VALUES
('lisa', uuid(),  'Playing my saxophone.');
INSERT INTO usershouts (username, usershout_id, body) VALUES
('maggie', uuid(), 'Goo goo ga ga!');

INSERT INTO shoutwall (username, shoutwall_id, posted_by, body) VALUES
('homer', uuid(), 'homer', 'Mmm... donuts');
INSERT INTO shoutwall (username, shoutwall_id, posted_by, body) VALUES
('marge', uuid(), 'marge', 'Time to clean the house!');
INSERT INTO shoutwall (username, shoutwall_id, posted_by, body) VALUES
('bart', uuid(), 'bart', 'Eat my shorts!');
INSERT INTO shoutwall (username, shoutwall_id, posted_by, body) VALUES
('lisa', uuid(), 'lisa', 'Playing my saxophone.');
INSERT INTO shoutwall (username, shoutwall_id, posted_by, body) VALUES
('maggie', uuid(), 'maggie', 'Goo goo ga ga!');

-- 2.6 CASSANDRA
-- Mettre à jour le mot de passe d'Homer
UPDATE utilisateurs
SET password = 'ohpinaise'
WHERE username = 'homer';
-- Homer commence à suivre Lisa
INSERT INTO following (username, followed) VALUES
('homer', 'lisa');
INSERT INTO followers (username, following) VALUES
('lisa', 'homer');
-- Homer arrête de suivre Bart
DELETE FROM following
WHERE username = 'homer' AND followed = 'bart';
DELETE FROM followers
WHERE username = 'bart' AND following = 'homer';
-- Maggie commence à suivre Bart
INSERT INTO following (username, followed) VALUES
('maggie', 'bart');
INSERT INTO followers (username, following) VALUES
('bart', 'maggie');
-- Marge n’est plus suivi par Homer
DELETE FROM following
WHERE username = 'homer' AND followed = 'marge';
DELETE FROM followers
WHERE username = 'marge' AND following = 'homer';
-- Mettre à jour le shout d'Homer
UPDATE shouts
SET body = 'Oh pinaise !'
WHERE shout_id = b6820517-5ddd-4e25-bad8-d60da1b17283;
-- Mettre à jour le shout d'Homer sur le mur
UPDATE shoutwall
SET body = 'Oh pinaise !'
WHERE shoutwall_id = 40eb6d37-a75a-4f9e-bf91-fb14313e305e;

-- 2.7 CASSANDRA
-- Ajoutez age dans la table users
ALTER TABLE utilisateurs 
ADD age int;
-- Mettre à jour l'age de Homer
UPDATE utilisateurs
SET age = 36
WHERE username = 'homer';

-- ERREUR
SELECT * FROM utilisateurs
WHERE age = 36;
-- Creer un index pour que cela fonctionne
CREATE INDEX ON utilisateurs(age);