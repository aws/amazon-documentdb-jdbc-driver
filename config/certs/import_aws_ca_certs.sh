mydir=~/certs
storepassword=$TRUSTSTORE_PASSWORD
if [ -f "$JAVA_HOME/jre/lib/security/cacerts" ]; then
  truststore="$JAVA_HOME/jre/lib/security/cacerts"
else
  truststore="$JAVA_HOME/lib/security/cacerts"
fi

# Clear the target directory
mkdir -p ${mydir}
rm -f ${mydir}/*

curl -sS "https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem" > ${mydir}/rds-combined-ca-bundle.pem
awk 'split_after == 1 {n++;split_after=0} /-----END CERTIFICATE-----/ {split_after=1}{print > "rds-ca-" n ".pem"}' < ${mydir}/rds-combined-ca-bundle.pem

for CERT in rds-ca-*; do
  alias=$(openssl x509 -noout -text -in $CERT | perl -ne 'next unless /Subject:/; s/.*(CN=|CN = )//; print')
  keytool -import -file ${CERT} -alias "${alias}" -storepass ${storepassword} -keystore "${truststore}" -noprompt
  rm $CERT
done

rm ${mydir}/rds-combined-ca-bundle.pem
