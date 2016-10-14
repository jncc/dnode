
import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { Hero } from './hero'
import { HeroService } from './hero.service'

@Component({
  selector: 'my-heroes',
  providers: [HeroService],
  templateUrl: 'heroes.component.html',
  styleUrls: [ 'heroes.component.css' ],
  moduleId: module.id
})
export class HeroesComponent implements OnInit{
  constructor(
    private heroService: HeroService,
    private router: Router ) {
  }

  heroes: Hero[];
  ngOnInit() {
    this.heroService.getHeroes().then(heroes => this.heroes = heroes);
  }

  selectedHero: Hero | null;
  onSelect(hero: Hero) {
    this.selectedHero = hero;
  }

  gotoDetail() {
    this.router.navigate(['/detail', this.selectedHero.id]);
  }
}
