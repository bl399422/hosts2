# Hosts 2

## About
Hosts2 functions as a decentralized DNS cache system. It does not exist to replace existing centralized infrastructure but instead exists as an emergency fallback for communications to resolve to the correct holder in the event of an outage. Service addresses are announced to the swarm and its propogated throughout the network. Even if a service does not announce itself it can still be referenced by other peers. Allowing the entire internet to be locally mapable. The idea is no one peer needs to know the entire network to arrive to a resolution. The data is stored on local machines within a Comma Seperated Value file hosts2.csv. Making it readable on any system and easy to manually transcribe or exchange by a human if necessary. Hosts2 is web browser extension both for portability and ease of integration. IP addresses associated with domains are verified by the TLS/SSL certificate supplied by the IP address meaning if a malicious peer inputs a false IP address into the table the extension will reject it on use.

## Client Setup
Load the unpacked extension found in `src`: https://developer.chrome.com/docs/extensions/get-started/tutorial/hello-world#load-unpacked

## Server Setup
In `server` run `npm i` to install required packages, then run `node index.js` to run the swarm server. For demo/test peers run `npm run swarm -- --peers=YOUR_VALUE --visits=YOUR_VALUE` (Recommended peers 15, visits 100)