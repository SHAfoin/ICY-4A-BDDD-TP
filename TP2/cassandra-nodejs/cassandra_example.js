const cassandra = require("cassandra-driver");
// Configuration du cluster
const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1", // Remplacez par votre nom de datacenter si différent
  keyspace: "tp2_cassandra",
});
async function run() {
  try {
    // Connectez-vous au cluster
    await client.connect();
    console.log("Connected to Cassandra");

    // Ajouter la colonne age à la table users
    alterTableQuery = "ALTER TABLE utilisateurs ADD age INT";
    await client.execute(alterTableQuery);
    console.log("Column age added to table users");
    alterTableQuery = "CREATE INDEX ON utilisateurs(age)";
    await client.execute(alterTableQuery);
    console.log("Index réalisé sur age");

    // Visualiser le contenu de la table users
    selectQuery = "SELECT * FROM utilisateurs";
    result = await client.execute(selectQuery);
    console.log("Users:");
    result.rows.forEach((row) => {
      console.log(row);
    });

    // Mettre à jour l'âge de Homer
    const updateAgeQuery =
      "UPDATE utilisateurs SET age = 36 WHERE username = 'homer'";
    await client.execute(updateAgeQuery);
    console.log("Homer's age updated to 36");

    // Afficher tous les utilisateurs suivis par un utilisateur spécifique
    username = process.argv[2];
    selectQuery = "SELECT username FROM following WHERE followed = ?";
    result = await client.execute(selectQuery, [username]);
    console.log("Personnes suivies par " + username + ":");
    result.rows.forEach((row) => {
      console.log(row);
    });

    // Afficher les utilisateurs qui suivent un utilisateur spécifique
    username = process.argv[3];
    selectQuery = "SELECT username FROM followers WHERE following = ?";
    result = await client.execute(selectQuery, [username]);
    console.log("Personnes qui suivent " + username + ":");
    result.rows.forEach((row) => {
      console.log(row);
    });

    // Afficher tous les usershoots
    selectQuery = "SELECT * FROM usershouts";
    result = await client.execute(selectQuery);
    console.log("Usershoots:");
    result.rows.forEach((row) => {
      console.log(row);
    });

    // Afficher les "shouts" d'un utilisateur spécifique
    username = process.argv[4];
    selectQuery = "SELECT * FROM usershouts WHERE username = ?";
    console.log("Shouts de " + username + ":");
    result = await client.execute(selectQuery, [username]);
    result.rows.forEach((row) => {
      console.log(row);
    });
  } catch (err) {
    console.error("There was an error", err);
  } finally {
    // Fermer la connexion
    await client.shutdown();
  }
}
run();
