from abbmatch.wiki_db import WikiDB
from wikipedia import WikipediaPage, PageError
from wikipedia.exceptions import DisambiguationError
from abbmatch import logger
from multiprocessing import Pool, cpu_count
import click


class WikiScraper:
    def __init__(self, starting_link=None, sqlite_path=None):
        """Class constructor

        Args:
            starting_link (str, optional): Initial topic from wikipedia to
                start scraping from. A suggestion would be to use
                "Lists_of_companies". Defaults to None.
            sqlite_path (str, optional): Path to the sqlite file where
                the data will be stored.
        """

        self.wiki_db = WikiDB() if sqlite_path is None else WikiDB(sqlite_path)
        self.wiki_db.create_database()
        if starting_link is not None:
            self.wiki_db.insert_one('link_map',
                                    values=(0, 0, -1, starting_link))

    def scrape_wiki(self, max_depth=6, batch_size=30):
        """Scrape the links from the wikipedia iteratively

        Args:
            max_depth (int, optional): Maximum depth to consider starting
                from the `starting_link`.  Defaults to 6.
            batch_size (int, optional): Number of items retrieved per
                iteration. Defaults to 30.
        """
        unvisited = self.wiki_db.get_unvisited_links(max_depth, batch_size)
        cpu_count_ = min(cpu_count(), batch_size)
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
                                            (id_link, '', ''))
                else:
                    logger.info("Storing content for: " + title)
                    self.wiki_db.insert_one('content', ('id_link', 'content', 'title'),
                                            (id_link, summary, title))
                    self.store_links(id_link, depth, links)

            # for id_link, link in unvisited:
            #     logger.info(F"Processing link: '{link}'")
            #     self.process_page(id_link, link)
            unvisited = self.wiki_db.get_unvisited_links(max_depth, batch_size)

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


@click.command()
@click.option('--starting_link', default=None,
              help='Initial topic from wikipedia to start scraping from')
@click.option('--batch_size', default=30,
              help='Batch size (for parallel processing)')
@click.option('--sqlite_path', default='abbmatch.db',
              help='Path to the sqlite file to store the data')
def run_scraper(starting_link, batch_size, sqlite_path):
    wiki = WikiScraper(starting_link, sqlite_path)
    wiki.scrape_wiki(batch_size=batch_size)


if __name__ == "__main__":
    run_scraper()
