ipaddr=$(curl https://api.ipify.org --silent)

echo $ipaddr

ret=$(curl -s -I 'https://stage-sonar.pwc.com' | grep -i HTTP | grep 200)
len=$(echo $ret| wc -c)
if [ $len -lt 2 ]
then
vars="{\"key\":\"lncmbr5kax\",\"ip\":\"$ipaddr\",\"type\":\"add\"}"
curl -k -d "$vars" -H 'Content-Type: application/json' -X POST 'https://us-central1-pg-us-e-app-268182.cloudfunctions.net/sqube'

# wait time for the ip to get into the whitelist
for counter in {1..20}
do
echo $counter
  sleep 30s
    ret=$(curl -s -I 'https://stage-sonar.pwc.com' | grep -i HTTP | grep 200)
    len=$(echo $ret| wc -c)
if [ $len -gt 1 ]
then
 break
fi
done
# safety buffer
sleep 30s
else
   echo "Already white listed"
fi
