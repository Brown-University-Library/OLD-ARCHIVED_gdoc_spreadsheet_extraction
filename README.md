#### Info... ####

- flow:
    - cron job calls script which
    - looks for entry in a google-doc spreadsheet that is ready to be ingested into our repository
    - if item is ready:
        - prepares data
        - validates data
            - halts if data is invalid and updates spreadsheet with errors
        - calls ingestion api to ingest the item into the repository
        - updates the spreadsheet with link repository link

- code contact: birkin_diana@brown.edu

---
