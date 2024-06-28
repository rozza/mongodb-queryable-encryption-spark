package demo;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.connection.MongoClientFactory;
import org.bson.BsonBinary;
import org.bson.BsonDocument;

import java.io.Serializable;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ClientEncryptionMongoClientFactory implements MongoClientFactory, Serializable {

    private final static String LOCAL_KEY_BASE_64 = "VZMn7NnR4AVvHF660gQ6itLKNcc9BxJiGHF9sPL1e1uIsX1WcRGzszgEbthgj19" +
            "ov4gsZEY53/Mz+BpEycHIw60y4txj3pmeVGogMCnrQbfozYKsLA8R/t07ALYhIfGI";
    private final MongoConfig config;

    /**
     * Create a new instance of MongoClientFactory
     *
     * @param config the MongoConfig
     */
    public ClientEncryptionMongoClientFactory(final MongoConfig config) {
        this.config = config;
    }


    @Override
    public MongoClient create() {
        System.out.println("============================");
        System.out.println("  CREATING CUSTOM CLIENT");
        System.out.println("============================");
        System.out.println("\n");

        // This would have to be the same master key as was used to create the encryption key
        byte[] localMasterKey = Base64.getDecoder().decode(LOCAL_KEY_BASE_64);
        Map<String, Map<String, Object>> kmsProviders =
                new HashMap<String, Map<String, Object>>() {{
                    put("local", new HashMap<String, Object>() {{
                        put("key", localMasterKey);
                    }});
                }};

        String keyVaultNamespace = "encryption.__keyVault";
        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder()
                        .applyConnectionString(config.getConnectionString())
                        .build())
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProviders)
                .build();

        BsonBinary dataKeyId;
        try (ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings)) {
            dataKeyId = clientEncryption.createDataKey("local", new DataKeyOptions());
        }
        String base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());

        final String dbName = config.getDatabaseName();
        final String collName = config.getCollectionName();
        AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProviders)
                .schemaMap(new HashMap<String, BsonDocument>() {{
                    put(dbName + "." + collName,
                            // Need a schema that references the new data key
                            BsonDocument.parse("{"
                                    + "  properties: {"
                                    + "    encryptedField: {"
                                    + "      encrypt: {"
                                    + "        keyId: [{"
                                    + "          \"$binary\": {"
                                    + "            \"base64\": \"" + base64DataKeyId + "\","
                                    + "            \"subType\": \"04\""
                                    + "          }"
                                    + "        }],"
                                    + "        bsonType: \"string\","
                                    + "        algorithm: \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\""
                                    + "      }"
                                    + "    }"
                                    + "  },"
                                    + "  \"bsonType\": \"object\""
                                    + "}"));
                }}).build();

        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .autoEncryptionSettings(autoEncryptionSettings)
                .build();
        return MongoClients.create(clientSettings);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClientEncryptionMongoClientFactory that = (ClientEncryptionMongoClientFactory) o;
        return config.equals(that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }

}
