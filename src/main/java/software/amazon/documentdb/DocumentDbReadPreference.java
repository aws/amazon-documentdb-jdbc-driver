package software.amazon.documentdb;

/**
 * The enumeration of read preferences for DocumentDb.
 */
public enum DocumentDbReadPreference {
    PRIMARY("primary"),
    PRIMARY_PREFERRED("primaryPreferred"),
    SECONDARY("secondary"),
    SECONDARY_PREFERRED("secondaryPreferred"),
    NEAREST("nearest");

    private final String name;

    /**
     * Constructor for a read preference.
     *
     * @param name The value of the read preference.
     */
    DocumentDbReadPreference(final String name) {
        this.name = name;
    }

    /**
     * Gets the string value of the read preference.
     *
     * @return The name of the read preference.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns DocumentDbReadPreference with a name that matches input string.
     * @param readPreferenceString name of the read preference.
     * @return DocumentDbReadPreference of string.
     */
    public static DocumentDbReadPreference fromString(final String readPreferenceString) {
        for (DocumentDbReadPreference readPreference: DocumentDbReadPreference.values()) {
            if (readPreference.name.equals(readPreferenceString)) {
                return readPreference;
            }
        }
        return null;
    }
}
