# CTA Server

This software periodically parses the Cross The Ages asset collection on IMX (using the IMX SDK), to
keep a Postgresql up-to-date.

Then, the following API end-points allow to get structured data:

- /stats : get statistics about the supply of each Cross The Ages cards.
- /collection: get the full list of cards in the collection
- /card: get details about a card of the collection
- /user: get the cards owned by an user
- /users: get the list of users who own at least a Cross The Ages asset.
