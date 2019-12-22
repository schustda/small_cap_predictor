from etl.ihub import Ihub


if __name__ == '__main__':
    # Step 1 get the symbols

    # Step 2 

    def update_posts(self, symbol_id):

        # first, pull the necessary symbol info
        symbol = self.get_value('symbol', symbol_id=symbol_id)
        ihub_code = self.get_value('ihub_code', symbol_id=symbol_id)
        num_pinned, num_posts = self._total_and_num_pinned(ihub_code)

        self.interval_time, self.original_time = time(), time()

        # calculate which post numbers are missing from the database
        posts_to_add = set(range(1, num_posts+1))
        already_added = set(self.get_list('existing_posts', symbol_id=symbol_id))
        posts_to_add -= already_added

        error_list = []
        total_posts_to_add = len(posts_to_add)
        print(f"Adding {total_posts_to_add} post(s) for {symbol} ({symbol_id})")
        while posts_to_add:
            post_number = max(posts_to_add)
            page = post_number
            while True:
                try:
                    page_df, error_list = self._get_page(ihub_code, post_number=page,
                                                         num_pinned=num_pinned,
                                                         error_list=error_list)
                    break

                # if the number one post is deleted and you're calling it, it will fail
                except ValueError:
                    page += 1

                except Exception as e:
                    print(f'{e} ERROR ON PAGE: {str(post_number)} for {ihub_code}')
                    error_list.append(post_number)
                    page_df = pd.DataFrame()
                    break

            page_df = self._add_deleted_posts(page_df, post_number)
            page_df['symbol_id'] = symbol_id
            self.to_table(page_df, 'ihub.message_board')
            posts_to_add -= set(page_df.post_number)
            if self.verbose:
                num = (total_posts_to_add-len(posts_to_add))
                total = total_posts_to_add
                self.status_update(num, total)
            if self.delay:
                sleep(randint(2, 15))
