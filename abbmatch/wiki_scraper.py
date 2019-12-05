from abbmatch.wiki_db import WikiDB
from wikipedia import WikipediaPage, PageError
from wikipedia.exceptions import DisambiguationError
from abbmatch import logger
from multiprocessing import Pool, cpu_count


class WikiScraper:
    def __init__(self, starting_link=None):
        """Class constructor

        Args:
            starting_link (str, optional): Initial topic from wikipedia to start the scraping.
                A suggestion would be to use "Lists_of_companies". Defaults to None.
        """
        self.wiki_db = WikiDB()
        self.wiki_db.create_database()
        if starting_link is not None:
            self.wiki_db.insert_one('link_map', values=(0, 0, -1, starting_link))

    def get_unvisited_links(self, max_depth=6, limit=30):
        """Get the list of links not visited from the database

        Args:
            max_depth (int, optional): Maximum depth to consider starting from the
                `starting_link`.  Defaults to 6.
            limit (int, optional): Maximum number of links to process in parallel per
                iteration. Defaults to 30.

        Returns:
            list((str, str, str)): List of links retrieved from the DB. The items
                are tuples (id_link, link, current_depth)
        """
        sql = F"""
            SELECT id, link, depth FROM link_map as A
            LEFT JOIN content as B ON A.id=B.id_link
            WHERE B.content is NULL AND depth <= {max_depth}
            ORDER BY depth, id LIMIT {limit}
        """
        return self.wiki_db._fetchall(sql)

    def scrape_wiki(self, max_depth=6, limit=30):
        """Scrape the links from the wikipedia iteratively

        Args:
            max_depth (int, optional): [description]. Defaults to 6.
            limit (int, optional): [description]. Defaults to 30.
        """
        unvisited = self.get_unvisited_links(max_depth, limit)
        cpu_count_ = cpu_count()
        while len(unvisited) > 0:
            logger.info(F"Processing {len(unvisited)} items in parallel")
            with Pool(processes=cpu_count_) as pool:
                # Each item from unvisited is composed by id, link, depth
                res = pool.map(process_page, unvisited)
            # Writing the results
            for status, (id_link, summary, depth, title, links) in res:
                if status == 0:
                    logger.info(F"Page '{title}' (id {id_link}) does not match any pages. Try another id!")
                    self.wiki_db.insert_one('content', ('id_link', 'content', 'title'),
                                            (id_link, None, None))
                else:
                    logger.info("Storing content for: " + title)
                    self.wiki_db.insert_one('content', ('id_link', 'content', 'title'),
                                            (id_link, summary, title))
                    self.store_links(id_link, depth, links)

            # for id_link, link in unvisited:
            #     logger.info(F"Processing link: '{link}'")
            #     self.process_page(id_link, link)
            unvisited = self.get_unvisited_links(max_depth, limit)

    def store_links(self, parent_id, depth, link_list):
        # self.wiki_db._insert_many()

        # sql = F"INSERT INTO link_map (parent_id, link) VALUES ({parent_id}, ?)"
        link_list = [(l,) for l in link_list]
        self.wiki_db.insert_many(table='link_map', col=('parent_id', 'depth', 'link'),
                                 value_placeholder=(parent_id, depth, None),
                                 values=link_list)
        # _insert_many(sql, link_list)


def process_page(params):
    id_link, link, depth = params
    # Get the wiki object for the link
    try:
        wiki_obj = WikipediaPage(link)
        links = wiki_obj.links
    except (PageError, KeyError, DisambiguationError):
        return 0, (id_link, None, None, link, None)
    title = wiki_obj.title
    summary = wiki_obj.summary[:300].replace("\"", "'")
    return 1, (id_link, summary, depth+1, title, links)
